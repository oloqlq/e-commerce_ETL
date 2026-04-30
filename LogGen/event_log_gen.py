"""
규칙 있는 랜덤 생성
- 시간대별 가중치
- 지역별 가중치
- 디바이스별 카테고리 선호도 가중치
- session context 고정
"""

import json
import time
import random
import boto3
import uuid
import os

from faker import Faker
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
fake = Faker("ko_KR")

STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")
REGION = os.getenv("REGION")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

USER_POOL = [f"USER-{i:03d}" for i in range(1, 101)]

# 유저별 세션 매핑
user_sessions = {}
session_counter = 1
for user in USER_POOL:
    n = random.randint(1, 5)
    sessions = []
    for _ in range(n):
        sessions.append(f"SESS-{session_counter:03d}")
        session_counter += 1
    user_sessions[user] = sessions

HOUR_WEIGHTS = [
    1, 1, 1, 1, 1, 2,   # 00~05시
    3, 5, 6, 6, 7, 8,   # 06~11시
    10, 9, 7, 7, 7, 8,  # 12~17시
    9, 10, 10, 9, 7, 4  # 18~23시
]

def validate_env():
    required = {
        "KINESIS_STREAM_NAME": STREAM_NAME,
        "REGION": REGION,
        "ACCESS_KEY": ACCESS_KEY,
        "SECRET_KEY": SECRET_KEY,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")

def get_sleep_time():
    """현재 시간대에 따라 전송 간격 조절"""
    hour = datetime.now().hour
    weight = HOUR_WEIGHTS[hour]
    # weight 높을수록 간격 짧게
    # weight 1 -> 약 5초, weight 10 -> 약 0.5초
    return max(0.5, 5.5 - (weight * 0.5))

ACTIONS = ["view", "click", "add_to_cart", "wishlist", "search", "purchase"]
ACTION_WEIGHTS = [0.30, 0.20, 0.12, 0.08, 0.05, 0.25]

PAGE_TYPES = {
    "view": ["category", "product_detail"],
    "click": ["category", "product_detail"],
    "add_to_cart": ["product_detail"],
    "wishlist": ["product_detail"],
    "search": ["home", "category"],
    "purchase": ["checkout", "order_complete"],
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
    return random.choices(regions, weights=weights, k=1)[0]

REFERRERS = ["search", "sns", "direct", "ad"]

CAMPAIGNS = {
    "CAMP-001": "여름세일",
    "CAMP-002": "신규가입",
    "CAMP-003": "재구매",
}

DEVICES = ["mobile", "desktop", "tablet"]
PLATFORMS = ["app", "web"]

# 카테고리별 item_id 매핑 (product_master 기준)
CATEGORY_ITEMS = {
    "전자기기": [f"ITEM-{i:03d}" for i in range(101, 116)],
    "패션": [f"ITEM-{i:03d}" for i in range(201, 216)],
    "식품": [f"ITEM-{i:03d}" for i in range(301, 316)],
    "스포츠": [f"ITEM-{i:03d}" for i in range(401, 416)],
    "생활용품": [f"ITEM-{i:03d}" for i in range(501, 516)],
}

# 디바이스별 카테고리 선호도
DEVICE_CATEGORY_WEIGHTS = {
    "mobile": {"전자기기": 10, "패션": 40, "식품": 20, "스포츠": 15, "생활용품": 15},
    "desktop": {"전자기기": 40, "패션": 20, "식품": 15, "스포츠": 15, "생활용품": 10},
    "tablet": {"전자기기": 20, "패션": 25, "식품": 20, "스포츠": 20, "생활용품": 15},
}

def get_weighted_item(device):
    weights_map = DEVICE_CATEGORY_WEIGHTS[device]
    categories = list(weights_map.keys())
    weights = list(weights_map.values())
    category = random.choices(categories, weights=weights, k=1)[0]
    item_id = random.choice(CATEGORY_ITEMS[category])
    return item_id

def generate_session_context():
    """
    세션 단위 공통 속성 고정
    - user_id
    - session_id
    - session_start_time
    - region
    - device
    - platform
    - referrer
    - campaign_id
    """
    is_member = random.random() > 0.2
    user_id = random.choice(USER_POOL) if is_member else None

    if is_member:
        session_id = random.choice(user_sessions[user_id])
    else:
        session_id = f"SESS-GUEST-{random.randint(1000, 9999):04d}"

    device = random.choice(DEVICES)
    platform = random.choice(PLATFORMS)
    region = get_weighted_region()
    referrer = random.choice(REFERRERS)
    campaign_id = f"CAMP-{random.randint(1, 3):03d}" if referrer == "ad" else None

    return {
        "user_id": user_id,
        "session_id": session_id,
        "session_start_time": datetime.now(),
        "device": device,
        "platform": platform,
        "region": region,
        "referrer": referrer,
        "campaign_id": campaign_id,
    }

def choose_session_flow():
    """
    purchase는 add_to_cart 이후에만 나오도록 구성
    """
    return random.choice([
        ["view"],
        ["view", "click"],
        ["view", "click", "wishlist"],
        ["view", "click", "add_to_cart"],
        ["view", "click", "add_to_cart", "purchase"],
        ["search", "view", "click"],
        ["search", "view", "click", "wishlist"],
        ["search", "view", "click", "add_to_cart"],
        ["search", "view", "click", "add_to_cart", "purchase"],
    ])

def build_event_timestamp(session_start_time, step_idx):
    """
    세션 시작 시각 기준으로 이벤트 타임스탬프를 순차 증가
    """
    if step_idx == 0:
        return session_start_time.isoformat()

    delta_seconds = random.randint(3, 12) * step_idx
    return (session_start_time + timedelta(seconds=delta_seconds)).isoformat()

def generate_event_from_context(ctx, action, step_idx, fixed_item_id=None, fixed_search_keyword=None):
    """
    세션 컨텍스트를 공유하면서 이벤트 생성
    """
    if action == "search":
        item_id = None
        search_keyword = fixed_search_keyword or random.choice(SEARCH_KEYWORDS)
    else:
        item_id = fixed_item_id or get_weighted_item(ctx["device"])
        search_keyword = None

    event = {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": build_event_timestamp(ctx["session_start_time"], step_idx),
        "user_id": ctx["user_id"],
        "session_id": ctx["session_id"],
        "item_id": item_id,
        "action": action,
        "device": ctx["device"],
        "platform": ctx["platform"],
        "region": ctx["region"],
        "referrer": ctx["referrer"],
        "campaign_id": ctx["campaign_id"],
        "search_keyword": search_keyword,
        "page_type": random.choice(PAGE_TYPES[action]),
    }
    return event

def generate_session_events():
    """
    세션 단위 이벤트 생성
    - 하나의 세션에서는 region/device/platform/referrer/campaign 고정
    - 상품 관련 플로우는 item_id 고정
    - search가 있으면 search_keyword 고정
    """
    ctx = generate_session_context()
    flow = choose_session_flow()

    events = []
    fixed_item_id = None
    fixed_search_keyword = None

    if "search" in flow:
        fixed_search_keyword = random.choice(SEARCH_KEYWORDS)

    # 상품 상세 플로우에서는 동일 상품 유지
    if any(action in flow for action in ["view", "click", "add_to_cart", "wishlist", "purchase"]):
        fixed_item_id = get_weighted_item(ctx["device"])

    for idx, action in enumerate(flow):
        event = generate_event_from_context(
            ctx=ctx,
            action=action,
            step_idx=idx,
            fixed_item_id=fixed_item_id if action != "search" else None,
            fixed_search_keyword=fixed_search_keyword if action == "search" else None,
        )
        events.append(event)

    return events

def send_to_kinesis(client, event):
    client.put_record(
        StreamName=STREAM_NAME,
        Data=(json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8"),
        PartitionKey=event["session_id"],
    )

def main():
    validate_env()

    kinesis = boto3.client(
        "kinesis",
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    print("이벤트 로그 스트리밍 시작...")
    while True:
        events = generate_session_events()

        for event in events:
            send_to_kinesis(kinesis, event)
            print(event)
            time.sleep(get_sleep_time())

if __name__ == "__main__":
    main()