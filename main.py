# phx_SctIvSyFVjlDidigURVcR0ZWBROyQrtPTuvuWY0bkiN
import asyncio
import json
import requests
import boto3
import consts

from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError

memphis = Memphis()

class MessageHandler():
    def create_and_put_bucket(self, bucket_name, msgs):
        try:
            client = boto3.client('s3', aws_access_key_id=consts.AWS_ACCESS_KEY, aws_secret_access_key=consts.AWS_SECRET_KEY)
            response = client.head_bucket(Bucket=bucket_name)
        except Exception as e:
            if e.response['Error']['Code'] == '404' and e.response['Error']['Message'] == 'Not Found':
                response_create_bucket = client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': 'eu-central-1'
                    },
                )
                if response_create_bucket['ResponseMetadata']['HTTPStatusCode'] != 200:
                    raise Exception("creation bucket failure")
                response_public = client.put_public_access_block(
                Bucket=bucket_name,
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': True,
                    'IgnorePublicAcls': True,
                    'BlockPublicPolicy': True,
                    'RestrictPublicBuckets': True
                    },
                )
                if response_public['ResponseMetadata']['HTTPStatusCode'] != 200:
                        raise Exception("creation provate access for bucket failure")  
        finally:
            try:
                client.put_object(Body=json.dumps(msgs), Bucket=bucket_name, Key= bucket_name+".json")
            except Exception as e:
                return e

    async def msg_handler_raw_to_transformed(self, msgs, error):
        try:
            if len(msgs) > 0 :
                my_msgs = []
                raw_msgs = msgs[0]
                message = raw_msgs.get_data()
                await raw_msgs.ack()
            
                dict_msg = json.loads(message.decode('utf-8'))
                for m in dict_msg['results']:
                    m['person']['is_identified'] = str(
                        m['person']['is_identified']).lower()
                    my_msgs.append(m)
    
                lambda_response = requests.post(url=consts.TRANSFORMED_LAMBDA_URL,
                                json={
                                    "payload": my_msgs,
                                }, headers={"content-type": "application/json"}
                                )
                if error:
                    return
        except (MemphisError, MemphisConnectError, MemphisHeaderError, Exception) as e:
            return
        finally:
            return

    async def msg_handler_transformed_to_enrich(self,msgs, error):
        try:            
            my_msgs = []
            for msg in msgs:
                message = msg.get_data()
                await msg.ack()
        
                dict_msg = json.loads(message.decode('utf-8')) 
                my_msgs.append(dict_msg)
           
            lambda_response = requests.post(url=consts.ENRICHED_LAMBDA_URL,
                            json={
                                "payload": my_msgs
                            }, headers={"content-type": "application/json"}
                            )
            if lambda_response.status_code != 200:
                raise Exception("lambda error")
            if error:
                    return
            
        except (MemphisError, MemphisConnectError, MemphisHeaderError, Exception) as e:
            return e
    
    async def msg_handler_upload_to_s3(self, msgs, error):
        try:
            faq_msgs, main_page_msgs, blog_msgs = [], [], []
            for msg in msgs:
                message = msg.get_data()
                await msg.ack()
                dict_msg = json.loads(message.decode('utf-8'))

                if 'type' in dict_msg:
                    if dict_msg['type'] == "blog":
                        blog_msgs.append(dict_msg)
                    elif dict_msg['type'] == "faq":
                        faq_msgs.append(dict_msg)
                    elif dict_msg['type'] == "main-page":
                        main_page_msgs.append(dict_msg)
            if error:
                    return

        except Exception as e:
            return e
        finally:
            if len(main_page_msgs) > 0 :
                self.create_and_put_bucket("main-page-py-data-meetup", main_page_msgs)
            if len(blog_msgs) > 0 :
                self.create_and_put_bucket("blog-py-data-meetup",blog_msgs)
            if len(faq_msgs) > 0 :
                self.create_and_put_bucket("faq-py-data-meetup",faq_msgs)
            return     


def get_raw_data():
    try:
        get_posthog_events = "https://app.posthog.com/api/projects/12503/events/?personal_api_key="+consts.API_KEY_POSTHOG
        res = requests.get(get_posthog_events)
        data = res.json()
        return data
    except Exception as e:
        return e

async def raw_step(data):
    try:
        raw_data = json.dumps(data).encode('utf-8')
        producer = await memphis.producer(station_name="raw", producer_name="p1")
        await producer.produce(bytearray(raw_data))
    except Exception as e:
        return e

async def transformed_step():
    try:
        consumer = await memphis.consumer(station_name="raw", consumer_name="consumer1", consumer_group="", batch_size=1000)
        message_handler = MessageHandler()
        consumer.consume(message_handler.msg_handler_raw_to_transformed)
        await asyncio.sleep(10)
        await consumer.destroy()
    except Exception as e:
        return e

async def enriched_step():
    try:
        message_handler = MessageHandler()
        consumer = await memphis.consumer(station_name="transformed", consumer_name="consumer2", consumer_group="", batch_size=1000)
        consumer.consume(message_handler.msg_handler_transformed_to_enrich)
        await asyncio.sleep(10)
        await consumer.destroy()
    except Exception as e:
        return e

async def save_to_s3_bucket():
    try:
        consumer = await memphis.consumer(station_name="enriched", consumer_name="consumer3", consumer_group="", batch_size=1000)
        message_handler = MessageHandler()
        consumer.consume(message_handler.msg_handler_upload_to_s3)
        await asyncio.sleep(10)
        await consumer.destroy()
    except Exception as e:
        return e

async def main():
    try:
        await memphis.connect(host=consts.HOST, username=consts.USER_NAME, connection_token=consts.CONNECTION_TOKEN)
        data = get_raw_data()
        await raw_step(data)
        await transformed_step()
        await enriched_step()
        await save_to_s3_bucket()
    except (MemphisError, MemphisConnectError, MemphisHeaderError) as e:
        return e
    finally:
        await memphis.close()

if __name__ == '__main__':
    asyncio.run(main())
