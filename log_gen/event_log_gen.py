'''
규칙 있는 랜덤 생성
    - 시간대별 가중치
    - 지역별 가중치
    - 디바이스별 카테고리 선호도 가중치
'''
import json
import time
import random
import boto3
import uuid
from faker import Faker
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()
fake = Faker("ko_KR")

STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")
REGION      = os.getenv("REGION")

USER_POOL = [f"USER-{i:03d}" for i in range(1, 101)]

# 유저별 세션 매핑
user_sessions = {}
session_counter = 1
for user in USER_POOL:
    n = random.randint(1, 5)
    sessions = []
    for _ in range(n):
        sessions.append(f"SESS-{session_counter:003d}")
        session_counter += 1
    user_sessions[user] = sessions

ITEM_IDS = [f"ITEM-{i:03d}" for i in range(1, 75)]

HOUR_WEIGHTS = [
    1, 1, 1, 1, 1, 2,   # 00~05시
    3, 5, 6, 6, 7, 8,   # 06~11시
    10, 9, 7, 7, 7, 8,  # 12~17시
    9, 10, 10, 9, 7, 4  # 18~23시
]

def get_sleep_time():
    """현재 시간대에 따라 전송 간격 조절"""
    hour   = datetime.now().hour
    weight = HOUR_WEIGHTS[hour]
    # weight 높을수록 간격 짧게 (트래픽 많게)
    # weight 1 → 5초, weight 10 → 0.5초
    return 5.5 - (weight * 0.5)

ACTIONS = ["view", "click", "add_to_cart", "wishlist", "search"]
ACTION_WEIGHTS = [0.50, 0.25, 0.12, 0.08, 0.05]  # 실제 이커머스 분포

# 어떤 페이지에서 action했는지
# Gold: page_type = product_detail에서 add_to_cart 비율 -> 상품 상세 페이지 전환율
PAGE_TYPES = {
    "view":        ["category", "product_detail"],
    "click":       ["category", "product_detail"],
    "add_to_cart": ["product_detail"],
    "wishlist":    ["product_detail"],
    "search":      ["home", "category"],
}

SEARCH_KEYWORDS = [
    "운동화", "웹캠", "닭가슴살", "요가매트", "무선 이어폰",
    "패딩", "단백질", "스마트워치", "청바지", "덤벨"
]

REGION_WEIGHTS = {
    "서울": 35,
    "경기": 25,
    "부산": 10,
    "인천": 10,
    "대구": 8,
    "광주": 6,
    "대전": 6,
}

def get_weighted_region():
    regions = list(REGION_WEIGHTS.keys())
    weights = list(REGION_WEIGHTS.values())
    return random.choices(regions, weights=weights)[0]

REFERRERS = ["search", "sns", "direct", "ad"]

# 광고
CAMPAIGNS = {
    "CAMP-001": "여름세일",
    "CAMP-002": "신규가입",
    "CAMP-003": "재구매",
}

DEVICES = ["mobile", "desktop", "tablet"]

# 카테고리별 item_id 매핑 (product_master 기준)
CATEGORY_ITEMS = {
    "전자기기": [f"ITEM-{i:03d}" for i in range(101,  116)],
    "패션":     [f"ITEM-{i:03d}" for i in range(201, 216)],
    "식품":     [f"ITEM-{i:03d}" for i in range(301, 316)],
    "스포츠":   [f"ITEM-{i:03d}" for i in range(401, 416)],
    "생활용품":     [f"ITEM-{i:03d}" for i in range(501, 516)],
}

# 디바이스별 카테고리 선호도
DEVICE_CATEGORY_WEIGHTS = {
    "mobile":  {"전자기기": 10, "패션": 40, "식품": 20, "스포츠": 15, "생활용품": 15},
    "desktop": {"전자기기": 40, "패션": 20, "식품": 15, "스포츠": 15, "생활용품": 10},
    "tablet":  {"전자기기": 20, "패션": 25, "식품": 20, "스포츠": 20, "생활용품": 15},
}

def get_weighted_item(device):
    weights_map = DEVICE_CATEGORY_WEIGHTS[device]
    categories  = list(weights_map.keys())
    weights     = list(weights_map.values())
    
    # 카테고리 먼저 선택 → 그 카테고리 상품 중 랜덤
    category = random.choices(categories, weights=weights)[0]
    item_id  = random.choice(CATEGORY_ITEMS[category])
    return item_id

PLATFORMS = ["app", "web"]

# 로그 생성
def generate_event(user_id, session_id, action=None):
    action    = action or random.choices(ACTIONS, weights=ACTION_WEIGHTS)[0]
    referrer = random.choice(REFERRERS)
    device = random.choice(DEVICES)

    # action에 따라 item_id / search_keyword 결정
    if action == "search":
        item_id        = None
        search_keyword = random.choice(SEARCH_KEYWORDS)
    else:
        item_id        = get_weighted_item(device)
        search_keyword = None

    events = {
        "event_id":        str(uuid.uuid4()),
        "event_timestamp": datetime.now().isoformat(),
        "user_id":         user_id if random.random() > 0.2 else None,
        "session_id":      session_id if user_id else f"SESS-{random.randint(1,999):03d}",
        "item_id":         item_id,
        "action":          action,
        "device":          device,
        "platform":        random.choice(PLATFORMS),
        "region":          get_weighted_region(),
        "referrer":        referrer,
        "campaign_id":    f"CAMP-{random.randint(1,3):03d}" if referrer == "ad" else None,
        "search_keyword":  search_keyword,
        "page_type":       random.choice(PAGE_TYPES[action]),
    }
    return events

# 세션 단위 이벤트 묶음 생성
def generate_session_events(user_id, session_id):
    flow = random.choice([
        ["view"],
        ["view", "click"],
        ["view", "click", "add_to_cart"],
        ["view", "click", "wishlist"],
        ["search", "view", "click"],
        ["search", "view", "click", "add_to_cart"],
    ])

    events = []
    for action in flow:
        event = generate_event(user_id, session_id, action)
        events.append(event)
    
    return events

# Kinesis 전송
def send_to_kinesis(client, event):
    client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event, ensure_ascii=False),
        PartitionKey=event["action"]
    )

def main():
    kinesis = boto3.client(
        "kinesis",
        region_name=REGION,
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_KEY"),
    )

    print("이벤트 로그 스트리밍 시작...")
    while True:
        # 유저 선택 -> 그 유저 세션 선택
        user_id = random.choice(USER_POOL)
        session_id = random.choice(user_sessions[user_id])

        events = generate_session_events(user_id, session_id)
        for event in events:
            send_to_kinesis(kinesis, event)
            print(event)
            time.sleep(get_sleep_time()) 

if __name__ == "__main__":
    main()