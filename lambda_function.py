from __future__ import print_function, unicode_literals
import urllib.request
import urllib.parse
import json
from base64 import b64decode
from logging import Formatter, StreamHandler, getLogger
import os
import traceback
import sys

import boto3

REGION_NAME = 'ap-northeast-1'

# ロガー初期化 start ####################################################
_LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG")
_logger = getLogger(__name__)
_logger.setLevel(_LOG_LEVEL)
formatter = Formatter(
    fmt="%(asctime)s:[%(filename)s](%(lineno)s)fn:%(funcName)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")

# stdout
s_handler = StreamHandler()
s_handler.setLevel(_LOG_LEVEL)
s_handler.setFormatter(formatter)
_logger.addHandler(s_handler)


def lambda_handler(event, context):
    _logger.info(f"{sys._getframe().f_code.co_name} Start.")
    try:
        config = _find_config()

        ec2_client = boto3.client('ec2', region_name=REGION_NAME)
        cloud_watch_client = boto3.client('cloudwatch',
                                          region_name=REGION_NAME)

        instance_info = _get_instance_info(ec2_client, config)

        ec2_alarm_info = _get_ec2_alarm(cloud_watch_client)

        check_result = _check_ec2_alarm(instance_info, ec2_alarm_info)

    except Exception as e:
        _logger.error(f"Error: {sys._getframe().f_code.co_name} ")
        _logger.error(traceback.format_exc())
        raise e
    finally:
        _logger.info(f"{sys._getframe().f_code.co_name} End.")


def _check_alert_settings_A(alarm_info, instance_type, EXCLUDE_INSTANCE_TYPE):
    return instance_type in EXCLUDE_INSTANCE_TYPE and \
            alarm_info['Threshold'] != 1.0 or \
            alarm_info['Period'] != 60 or \
            alarm_info['EvaluationPeriods'] != 2 or \
            (alarm_info['ComparisonOperator'] != 'GreaterThanThreshold' and alarm_info['ComparisonOperator'] != 'GreaterThanOrEqualToThreshold')


def _check_alert_settings_B(alarm_info, instance_type, EXCLUDE_INSTANCE_TYPE):
    return instance_type not in EXCLUDE_INSTANCE_TYPE and \
            'arn:aws:automate:ap-northeast-1:ec2:recover' not in alarm_info['AlarmActions'] or\
            alarm_info['Threshold'] != 1.0 or \
            alarm_info['Period'] != 60 or\
            alarm_info['EvaluationPeriods'] != 2 or\
            (alarm_info['ComparisonOperator'] != 'GreaterThanThreshold' and alarm_info['ComparisonOperator'] != 'GreaterThanOrEqualToThreshold')


def _check_alert_settings_C(alarm_info):
    print('arn:aws:automate:ap-northeast-1:ec2:recover' not in
          alarm_info['AlarmActions'])
    return 'arn:aws:automate:ap-northeast-1:ec2:recover' not in alarm_info['AlarmActions'] or\
            alarm_info['Threshold'] != 1.0 or \
            alarm_info['Period'] != 60 or\
            alarm_info['EvaluationPeriods'] != 3 or\
            (alarm_info['ComparisonOperator'] != 'GreaterThanThreshold' and alarm_info['ComparisonOperator'] != 'GreaterThanOrEqualToThreshold')


def _check_ec2_alarm(config, instance_info, ec2_alarm_info):
    CHECK_ALARM_ACTION = config["CHECK_ALARM_ACTION"]
    EXCLUDE_INSTANCE_TYPE = config["EXCLUDE_INSTANCE_TYPE"]
    check_result = {
        "wrong_destination": [],
        "wrong_setting": {},
        "no_recover": {},
        "no_alarm": {}
    }
    for instance in instance_info:
        check_result["no_alarm"][instance] = instance_info[instance][
            "check_metric_names"]

    for alarm_name, alarm_info in ec2_alarm_info.items():
        checkAction = False

        for AlarmAction in alarm_info['AlarmActions']:
            if AlarmAction in CHECK_ALARM_ACTION:
                checkAction = True

        instance_id = alarm_info['InstanceId']
        if instance_id in instance_info:
            if not checkAction:
                # アラームの宛先にinfra-alert@がない
                check_result["wrong_destination"].append(alarm_name)

            if alarm_info['MetricName'] == 'StatusCheckFailed_System':
                instance_type = instance_info[instance_id]["type"]
                if len(EXCLUDE_INSTANCE_TYPE) != 0:
                    if _check_alert_settings_A(alarm_info, instance_type,
                                               EXCLUDE_INSTANCE_TYPE):
                        # アラーム設定が間違い
                        check_result["wrong_setting"][alarm_name] = alarm_info

                    elif _check_alert_settings_B(alarm_info, instance_type,
                                                 EXCLUDE_INSTANCE_TYPE):
                        # アラーム設定が間違い かつ アラームのアクションにrecoverが含まれない
                        check_result["no_recover"][alarm_name] = alarm_info
                elif len(EXCLUDE_INSTANCE_TYPE) == 0:
                    print("hello")
                    if _check_alert_settings_C(alarm_info):
                        # アラーム設定が間違い かつ アラームのアクションにrecoverが含まれない
                        check_result["no_recover"][alarm_name] = alarm_info

            if alarm_info['MetricName'] in instance_info[instance_id][
                    "check_metric_names"]:
                check_result["no_alarm"][instance_id].remove(
                    alarm_info['MetricName'])

    return check_result


def _get_ec2_alarm(cloud_watch_client):
    _logger.info(f"{sys._getframe().f_code.co_name} Start.")
    try:
        ec2_alarm_info = {}
        cloud_watch_paginator = cloud_watch_client.get_paginator(
            'describe_alarms')

        page_iterator = cloud_watch_paginator.paginate()
        for page in page_iterator:
            alarms = page['MetricAlarms']

            for alarm in alarms:

                if alarm['Namespace'] == 'AWS/EC2':
                    ec2_alarm_info[alarm['AlarmName']] = {
                        'AlarmActions': alarm['AlarmActions'],
                        'MetricName': alarm['MetricName'],
                        'Threshold': alarm['Threshold'],
                        'Period': alarm['Period'],
                        'EvaluationPeriods': alarm['EvaluationPeriods'],
                        'ComparisonOperator': alarm['ComparisonOperator'],
                        'InstanceId': alarm['Dimensions'][0]['Value']
                    }

        return ec2_alarm_info
    finally:
        _logger.info(f"{sys._getframe().f_code.co_name} End.")


def _get_instance_info(ec2_client, config) -> dict:
    _logger.info(f"{sys._getframe().f_code.co_name} Start.")
    instance_info_dict = {}
    check_metric_names = config["CHECK_METRIC_NAMES"]
    try:
        paginator = ec2_client.get_paginator('describe_instances')
        page_iterator = paginator.paginate(Filters=[{
            'Name': 'instance-state-name',
            'Values': [
                'running',
            ]
        }], )
        # instanceIDを取得
        for page in page_iterator:
            for reservation in page["Reservations"]:
                for instance in reservation["Instances"]:
                    instance_id = instance["InstanceId"]
                    instance_type = instance["InstanceType"]
                    # Tagsを見て、CheckAlarmSettingがNOならば除外(存在しない、またはYESなら追加
                    check_flag = True
                    Tags = instance["Tags"]
                    for tag in Tags:
                        if tag['Key'] == 'CheckAlarmSetting':
                            if tag['Value'] == 'NO':
                                check_flag = False

                    if check_flag:
                        instance_info_dict[instance_id] = {
                            "check_metric_names": check_metric_names,
                            "type": instance_type
                        }

        return instance_info_dict

    finally:
        _logger.info(f"{sys._getframe().f_code.co_name} End.")


def _send_message(message: str, room_id: str, api_token: str):
    """
    @param name_servers:通知必要のドメインの情報
    @param ns_record:ns_record変更したドメイン
    @param room_id:chartworkのroomid
    @param api_token:chartworkのapikey
    """
    _logger.info("Start")
    try:
        post_message_url = f"https://api.chatwork.com/v2/rooms/{room_id}/messages"
        headers = {'X-ChatWorkToken': api_token}
        data = urllib.parse.urlencode({'body': message})
        data = data.encode('utf-8')

        # リクエストの生成と送信
        request = urllib.request.Request(post_message_url,
                                         data=data,
                                         method="POST",
                                         headers=headers)
        with urllib.request.urlopen(request) as response:
            response_body = response.read().decode("utf-8")
            _logger.info(response_body)

    finally:
        _logger.info("End")


def _find_config():
    """
    Configファイルの情報を取得
    @return Config情報の配列
    """
    _logger.info(f"{sys._getframe().f_code.co_name} Start.")
    try:
        _CONFIG_FILE_NAME = os.environ.get("ENV_FILE")
        with open(f"env/{_CONFIG_FILE_NAME}", "r", encoding="utf-8") as file:
            config = json.load(file)

        return config

    finally:
        _logger.info(f"{sys._getframe().f_code.co_name} End.")
