"""
네이버 금융 주가 크롤러 (Naver Finance Crawler)
================================================
네이버 금융 웹 페이지에서 국내 주가(KOSPI/KOSDAQ) 데이터를 수집합니다.

주요 기능:
    NaverFinanceCrawler  – 특정 종목의 일별 OHLCV 데이터 수집
    get_market_stocks    – KOSPI 또는 KOSDAQ 상위 종목 목록 조회
    get_stock_info       – 종목 기본 정보 조회

사용 예시::

    crawler = NaverFinanceCrawler()
    df = crawler.get_daily_ohlcv("005930", pages=5)   # 삼성전자 25거래일
    print(df.head())

필요 패키지:
    pip install requests beautifulsoup4 pandas lxml
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.naver.com",
}

# ---------------------------------------------------------------------------
# 일별 시세 크롤러
# ---------------------------------------------------------------------------

class NaverFinanceCrawler:
    """
    네이버 금융에서 종목 일별 OHLCV 시세를 수집하는 크롤러.

    Parameters
    ----------
    delay : float
        요청 간 지연 시간(초). 서버 부하 방지 (기본 0.3초)
    """

    BASE_URL = "https://finance.naver.com/item/sise_day.nhn"
    INFO_URL = "https://finance.naver.com/item/main.nhn"
    MARKET_URL = "https://finance.naver.com/sise/sise_market_sum.nhn"

    def __init__(self, delay: float = 0.3) -> None:
        self.delay = delay
        try:
            import requests
            from bs4 import BeautifulSoup
            self._requests = requests
            self._bs4 = BeautifulSoup
            self._available = True
        except ImportError as e:
            logger.warning("크롤러 의존성 없음: %s  (pip install requests beautifulsoup4)", e)
            self._available = False

    def _check(self) -> None:
        if not self._available:
            raise ImportError(
                "pip install requests beautifulsoup4 lxml 을 먼저 실행하세요."
            )

    def get_daily_ohlcv(
        self,
        ticker: str,
        pages: int = 10,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        종목 일별 OHLCV 데이터 수집.

        Parameters
        ----------
        ticker     : 종목 코드 (예: "005930" – 삼성전자)
        pages      : 수집할 페이지 수 (1페이지 ≈ 25거래일)
        start_date : 시작일 필터 "YYYY.MM.DD" (선택)
        end_date   : 종료일 필터 "YYYY.MM.DD" (선택)

        Returns
        -------
        pd.DataFrame  컬럼: Date, Open, High, Low, Close, Volume
        """
        self._check()
        rows: list[dict] = []

        for page in range(1, pages + 1):
            try:
                resp = self._requests.get(
                    self.BASE_URL,
                    params={"code": ticker, "page": page},
                    headers=_HEADERS,
                    timeout=10,
                )
                resp.raise_for_status()
            except Exception as exc:
                logger.warning("페이지 %d 요청 실패: %s", page, exc)
                break

            soup = self._bs4(resp.content, "lxml")
            table = soup.find("table", class_="type2")
            if table is None:
                logger.debug("표 없음 – 페이지 %d", page)
                break

            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 7:
                    continue
                texts = [td.get_text(strip=True).replace(",", "") for td in tds]
                date_txt = texts[0]
                if not date_txt or not date_txt[0].isdigit():
                    continue
                try:
                    rows.append({
                        "Date":   date_txt,
                        "Close":  int(texts[1]) if texts[1] else None,
                        "Open":   int(texts[3]) if texts[3] else None,
                        "High":   int(texts[4]) if texts[4] else None,
                        "Low":    int(texts[5]) if texts[5] else None,
                        "Volume": int(texts[6]) if texts[6] else None,
                    })
                except (ValueError, IndexError):
                    continue

            time.sleep(self.delay)

        if not rows:
            return pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Volume"])

        df = pd.DataFrame(rows).dropna()
        df["Date"] = pd.to_datetime(df["Date"], format="%Y.%m.%d")
        df = df.sort_values("Date").reset_index(drop=True)

        if start_date:
            df = df[df["Date"] >= pd.to_datetime(start_date, format="%Y.%m.%d")]
        if end_date:
            df = df[df["Date"] <= pd.to_datetime(end_date, format="%Y.%m.%d")]

        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = df[col].astype(float)

        logger.info("종목 %s – %d 행 수집 완료", ticker, len(df))
        return df

    def get_stock_info(self, ticker: str) -> dict:
        """
        종목 기본 정보 조회 (현재가, 종목명, 등락률 등).

        Parameters
        ----------
        ticker : 종목 코드

        Returns
        -------
        dict
        """
        self._check()
        try:
            resp = self._requests.get(
                self.INFO_URL,
                params={"code": ticker},
                headers=_HEADERS,
                timeout=10,
            )
            resp.raise_for_status()
            resp.encoding = "euc-kr"
        except Exception as exc:
            logger.warning("종목 정보 요청 실패: %s", exc)
            return {"ticker": ticker, "error": str(exc)}

        soup = self._bs4(resp.content, "lxml")

        def _text(sel: str) -> str:
            el = soup.select_one(sel)
            return el.get_text(strip=True) if el else ""

        name   = _text("div.wrap_company h2 a")
        price  = _text("p.no_today .blind")
        change = _text("p.no_exday .blind")
        rate   = _text("p.no_exday em.nv01 .blind, p.no_exday em.nv02 .blind")

        return {
            "ticker": ticker,
            "name":   name,
            "price":  price.replace(",", "") if price else "",
            "change": change.replace(",", "") if change else "",
            "rate":   rate,
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
        }


# ---------------------------------------------------------------------------
# 시장 종목 목록 (KOSPI / KOSDAQ)
# ---------------------------------------------------------------------------

def get_market_stocks(
    market: str = "kospi",
    pages: int = 3,
    delay: float = 0.3,
) -> pd.DataFrame:
    """
    KOSPI 또는 KOSDAQ 시가총액 상위 종목 목록을 수집합니다.

    Parameters
    ----------
    market : "kospi" 또는 "kosdaq"
    pages  : 수집할 페이지 수 (1페이지 ≈ 50종목)
    delay  : 요청 간 지연 (초)

    Returns
    -------
    pd.DataFrame  컬럼: Rank, Name, Ticker, Price, MarketCap, …
    """
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError as exc:
        raise ImportError("pip install requests beautifulsoup4 lxml") from exc

    sosok = "0" if market.lower() == "kospi" else "1"
    BASE  = "https://finance.naver.com/sise/sise_market_sum.nhn"
    rows: list[dict] = []

    for page in range(1, pages + 1):
        try:
            resp = requests.get(
                BASE,
                params={"sosok": sosok, "page": page},
                headers=_HEADERS,
                timeout=10,
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("시장 목록 페이지 %d 실패: %s", page, exc)
            break

        soup = BeautifulSoup(resp.content.decode("euc-kr", errors="replace"), "lxml")
        table = soup.find("table", class_="type_2")
        if table is None:
            break

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 6:
                continue
            # 종목 링크에서 코드 추출
            a_tag = tds[1].find("a")
            if a_tag is None:
                continue
            href   = a_tag.get("href", "")
            ticker = href.split("code=")[-1] if "code=" in href else ""
            name   = a_tag.get_text(strip=True)
            texts  = [td.get_text(strip=True).replace(",", "") for td in tds]
            rows.append({
                "Name":       name,
                "Ticker":     ticker,
                "Price":      texts[2] if len(texts) > 2 else "",
                "Change":     texts[3] if len(texts) > 3 else "",
                "ChangeRate": texts[4] if len(texts) > 4 else "",
                "Volume":     texts[5] if len(texts) > 5 else "",
                "MarketCap":  texts[6] if len(texts) > 6 else "",
                "Market":     market.upper(),
            })
        time.sleep(delay)

    if not rows:
        return pd.DataFrame(columns=["Name", "Ticker", "Price", "Market"])

    df = pd.DataFrame(rows)
    df = df[df["Ticker"].str.match(r"^\d{6}$", na=False)].reset_index(drop=True)
    return df
