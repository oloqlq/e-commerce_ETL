import json
import random
import boto3
import uuid
import os
import time

from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")
REGION = os.getenv("REGION")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

USER_POOL = [f"USER-{i:03d}" for i in range(1, 101)]

USER_SESSIONS = {
    user: [f"SESS-{idx:03d}" for idx in range((i - 1) * 5 + 1, i * 5 + 1)]
    for i, user in enumerate(USER_POOL, start=1)
}

HOUR_WEIGHTS = [
    1, 1, 1, 1, 1, 2,
    3, 5, 6, 6, 7, 8,
    10, 9, 7, 7, 7, 8,
    9, 10, 10, 9, 7, 4
]

PAGE_TYPES = {
    "view": ["category", "product_detail"],
    "click": ["category", "product_detail"],
    "add_to_cart": ["product_detail"],
    "wishlist": ["product_detail"],
    "search": ["home", "category"],
    "purchase": ["checkout", "order_complete"],
}

SESSION_FLOWS = [
    ["view"],
    ["view", "click"],
    ["view", "click", "wishlist"],
    ["view", "click", "add_to_cart"],
    ["view", "click", "add_to_cart", "purchase"],
    ["search", "view", "click"],
    ["search", "view", "click", "wishlist"],
    ["search", "view", "click", "add_to_cart"],
    ["search", "view", "click", "add_to_cart", "purchase"],
]

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

CATEGORY_ITEMS = {
    "전자기기": [f"ITEM-{i:03d}" for i in range(101, 116)],
    "패션": [f"ITEM-{i:03d}" for i in range(201, 216)],
    "식품": [f"ITEM-{i:03d}" for i in range(301, 316)],
    "스포츠": [f"ITEM-{i:03d}" for i in range(401, 416)],
    "생활용품": [f"ITEM-{i:03d}" for i in range(501, 516)],
}

DEVICE_CATEGORY_WEIGHTS = {
    "mobile": {"전자기기": 10, "패션": 40, "식품": 20, "스포츠": 15, "생활용품": 15},
    "desktop": {"전자기기": 40, "패션": 20, "식품": 15, "스포츠": 15, "생활용품": 10},
    "tablet": {"전자기기": 20, "패션": 25, "식품": 20, "스포츠": 20, "생활용품": 15},
}

REFERRERS = ["search", "sns", "direct", "ad"]
DEVICES = ["mobile", "desktop", "tablet"]
PLATFORMS = ["app", "web"]

PAYMENT_METHODS = ["card", "kakao_pay", "naver_pay", "bank_transfer"]
PAYMENT_WEIGHTS = [0.55, 0.20, 0.20, 0.05]

DISCOUNTS = [0, 1000, 2000, 3000, 5000, 10000]
DISCOUNT_WEIGHTS = [0.45, 0.15, 0.15, 0.10, 0.10, 0.05]

SLEEP_INTERVAL = 0.5  # 배치 전송 간격 (초), 필요에 따라 조정
MAX_RETRIES = 3        # Kinesis 전송 실패 시 최대 재시도 횟수


def weighted_choice(weight_dict):
    return random.choices(
        list(weight_dict.keys()),
        weights=list(weight_dict.values()),
        k=1
    )[0]


def validate_env():
    missing = [
        key for key, value in {
            "KINESIS_STREAM_NAME": STREAM_NAME,
            "REGION": REGION,
            "ACCESS_KEY": ACCESS_KEY,
            "SECRET_KEY": SECRET_KEY,
        }.items()
        if not value
    ]

    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")


def get_session_start_time():
    now = datetime.now()
    hour = random.choices(range(24), weights=HOUR_WEIGHTS, k=1)[0]

    session_start = datetime(
        now.year,
        now.month,
        now.day,
        hour,
        random.randint(0, 59),
        random.randint(0, 59)
    )

    return session_start if session_start <= now else session_start - timedelta(days=1)


def get_item_id(device):
    category = weighted_choice(DEVICE_CATEGORY_WEIGHTS[device])
    return random.choice(CATEGORY_ITEMS[category])


def create_session_context():
    is_member = random.random() > 0.2
    user_id = random.choice(USER_POOL) if is_member else None

    referrer = random.choice(REFERRERS)

    return {
        "user_id": user_id,
        "session_id": random.choice(USER_SESSIONS[user_id]) if user_id else f"SESS-GUEST-{random.randint(1000, 9999)}",
        "session_start_time": get_session_start_time(),
        "device": random.choice(DEVICES),
        "platform": random.choice(PLATFORMS),
        "referrer": referrer,
        "campaign_id": f"CAMP-{random.randint(1, 3):03d}" if referrer == "ad" else None,
        "region": weighted_choice(REGION_WEIGHTS),
    }


def create_event(ctx, action, step_idx, item_id, search_keyword):
    is_purchase = action == "purchase"

    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": (
            ctx["session_start_time"] + timedelta(seconds=random.randint(3, 12) * step_idx)
        ).isoformat(),
        "user_id": ctx["user_id"],
        "session_id": ctx["session_id"],
        "item_id": None if action == "search" else item_id,
        "action": action,
        "device": ctx["device"],
        "platform": ctx["platform"],
        "region": ctx["region"] if is_purchase else None,
        "referrer": ctx["referrer"],
        "campaign_id": ctx["campaign_id"],
        "search_keyword": search_keyword if action == "search" else None,
        "page_type": random.choice(PAGE_TYPES[action]),
        "discount_amount": random.choices(DISCOUNTS, weights=DISCOUNT_WEIGHTS, k=1)[0] if is_purchase else None,
        "payment_method": random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0] if is_purchase else None,
    }


def generate_session_events():
    ctx = create_session_context()
    flow = random.choice(SESSION_FLOWS)

    item_id = get_item_id(ctx["device"])
    search_keyword = random.choice(SEARCH_KEYWORDS) if "search" in flow else None

    return [
        create_event(ctx, action, idx, item_id, search_keyword)
        for idx, action in enumerate(flow)
    ]


def send_batch_to_kinesis(client, events):
    """이벤트 목록을 put_records로 한 번에 전송. 실패 시 MAX_RETRIES만큼 재시도."""
    records = [
        {
            "Data": (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8"),
            "PartitionKey": event["session_id"],
        }
        for event in events
    ]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.put_records(StreamName=STREAM_NAME, Records=records)

            # put_records는 일부 레코드만 실패할 수 있어서 별도 확인 필요
            failed_count = response.get("FailedRecordCount", 0)
            if failed_count == 0:
                return  # 전체 성공

            print(f"[경고] {failed_count}개 레코드 전송 실패 (시도 {attempt}/{MAX_RETRIES})")

        except Exception as e:
            print(f"[에러] Kinesis 전송 실패 (시도 {attempt}/{MAX_RETRIES}): {e}")

        if attempt < MAX_RETRIES:
            time.sleep(2 ** attempt)  # 재시도 간격: 2초, 4초, 8초 (지수 백오프)

    print("[에러] 최대 재시도 초과, 해당 배치 건너뜀")


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
        send_batch_to_kinesis(kinesis, events)

        for event in events:
            print(event)
        time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    main()