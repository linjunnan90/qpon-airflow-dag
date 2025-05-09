# airflow_config/airflow_tt_send.py
import json
import requests
import logging
from datetime import datetime


class TtSend:
    def __init__(self, sendStr, send_url):
        self.sendStr = sendStr
        self.send_url = send_url

    def stream_upload(self, url, data, chunk_size=1024 * 1024):
        with requests.post(
            url,
            data=self._streamer(data, chunk_size),
            headers={
                "Transfer-Encoding": "chunked",
                "contentType": "application/json",
                "Content-Type": "text/plain",
                "accept": "*/*",
                "Host": "mtp.myoas.com"
            }
        ) as resp:
            if resp.status_code == 200:
                print("数据已完成告警")
                print(resp.content)
            else:
                print("请求接口失败!")
                print(resp.content)

    def _streamer(self, data, chunk_size):
        for chunk in self._chunked(data, chunk_size):
            yield chunk.encode()

    def _chunked(self, data, chunk_size):
        pos = 0
        while pos < len(data):
            yield data[pos:pos + chunk_size]
            pos += chunk_size

    def sendTT(self):
        if len(self.sendStr) >= 5000:
            self.sendStr = self.sendStr[:4980] + "(告警超过5K个字符)"
        try:
            post_data = {"content": "@ALL" + self.sendStr}
            self.stream_upload(url=self.send_url, data=json.dumps(post_data))
        except Exception as e:
            print(e)


def send_failure_alert_factory(send_url: str):
    """
    返回一个可用于 Airflow DAG 的 on_failure_callback 函数
    """
    def send_failure_alert(context):
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        execution_date = context["execution_date"]
        error_message = str(context.get("exception", "Unknown error"))

        message = {
            "告警": f"{execution_date} 的 dag_id = {dag_id},task_id = {task_id} 任务执行失败!"
        }

        TtSend(str(message), send_url).sendTT()

    return send_failure_alert

def send_tt_alert_factory(message: str, send_url: str):
    """
    返回一个可用于 Airflow DAG 的 on_failure_callback 函数
    """
    TtSend(str(message), send_url).sendTT()

    return send_tt_alert