import requests
import openpyxl
import json

APP_ID = 'cli_a650ef04aebdd013'
APP_SECRET = '1qlq91kTVhqTJHpTGMCXjdTltPByG62j'
APP_TOKEN = 'MvbjblO3LaBb6UssD87cXF6dnOg'
TABLE_ID = 'tblAts3qHUAyI5WP'
EXCEL_PATH = "/Users/weibicheng/Desktop/python脚本/数据清洗/xhsRPAtest.xlsx"
FIELD_TYPE_CONFIG_PATH = 'field_config.json'

#  获取 tenant_access_token
def get_tenant_access_token():
    url = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal'
    payload = {
        "app_id": APP_ID,
        "app_secret": APP_SECRET
    }
    resp = requests.post(url, json=payload).json()
    return resp['tenant_access_token']

#  获取表格字段名 → field_id 映射
def get_field_id_mapping(token):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/fields"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    resp = requests.get(url, headers=headers).json()
    result = {}
    for field in resp['data']['items']:
        result[field['field_name']] = field['field_id']
    return result

#  读取 Excel
def read_excel(path, sheet_name='Sheet2'):
    wb = openpyxl.load_workbook(path)
    ws = wb[sheet_name]  # 指定读取Sheet2
    rows = list(ws.iter_rows(values_only=True))
    headers = rows[0]
    data = rows[1:]
    return headers, data

#  字段值转换
def convert_value(value, field_type):
    if value is None:
        return ""
    if field_type == "Text":
        return str(value)
    elif field_type == "Number":
        try:
            return float(value)
        except:
            return 0
    elif field_type == "Url":
        return {
            "link": str(value),
            "text": str(value)
        }
    elif field_type == "Date":
        return str(value)
    elif field_type == "Checkbox":
        return bool(value) if isinstance(value, bool) else str(value).strip() in ["1", "True", "是", "Y", "yes"]
    elif field_type == "SingleSelect":
        return str(value)
    elif field_type == "MultiSelect":
        if isinstance(value, str):
            return [v.strip() for v in value.split(',') if v.strip()]
        elif isinstance(value, list):
            return value
        else:
            return [str(value)]
    elif field_type == "User":
        return [str(value)] if not isinstance(value, list) else value
    else:
        return str(value)

#  写入数据到多维表格
def write_to_bitable(token, field_type_map, rows):
    """
    写入飞书多维表格
    :param token: 鉴权 Token
    :param field_type_map: 字段名 => 字段类型 的映射
    :param rows: list of dict，每一行为表格数据
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"

    headers_req = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    for row in rows:
        if not isinstance(row, dict):
            raise ValueError(f"每一行必须是 dict 类型，但实际是: {type(row)}")

        adapted_fields = {}
        for field_name, value in row.items():
            field_type = field_type_map.get(field_name, "Text")
            adapted_fields[field_name] = convert_value(value, field_type)

        payload = {
            "fields": adapted_fields
        }

        resp = requests.post(url, headers=headers_req, json=payload)
        print(f"插入记录: {resp.status_code}, {resp.text}")

        if resp.status_code != 200:
            print(f"插入失败: {row}")


#  主程序
if __name__ == "__main__":
    token = get_tenant_access_token()

    # 加载字段类型配置
    with open(FIELD_TYPE_CONFIG_PATH, 'r', encoding='utf-8') as f:
        field_type_map = json.load(f)

    # 读取 Excel
    headers, data = read_excel(EXCEL_PATH)

    # 将元组数据转换为字典列表
    rows = []
    for row_data in data:
        row_dict = {}
        for i, header in enumerate(headers):
            if i < len(row_data):
                row_dict[header] = row_data[i]
        rows.append(row_dict)

    # 写入数据
    write_to_bitable(token, field_type_map, rows)