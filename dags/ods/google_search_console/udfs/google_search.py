import heapq

import requests
import searchconsole
from config import DISCORD_WEBHOOK

TOPK = 5


class GoogleSearchConsoleReporter(object):
    def __init__(self):
        self.top_k_ctr = []
        self.top_k_position = []
        self.top_k_clicks = []
        self.top_k_impressions = []
        for top_k_heap in (
            self.top_k_ctr,
            self.top_k_position,
            self.top_k_clicks,
            self.top_k_impressions,
        ):
            heapq.heapify(top_k_heap)
        self.report = None

    def main(self):
        report_msg = self._get_report()
        self._send_report(report_msg)

    def _get_report(self):
        account = searchconsole.authenticate(
            client_config="/usr/local/airflow/dags/client_secret_google_search_console.json",
            credentials="/usr/local/airflow/dags/client_secret_google_search_console_serialized.json",
        )
        webproperty = account["https://tw.pycon.org/"]
        return webproperty.query.range("today", days=-7).dimension("query").get()

    def _send_report(self, report_msg):
        self._maitain_topk_heap(report_msg)
        msg_heap_dict = {
            f"透過 google 搜尋點進 PyConTW 官網的所有關鍵字，ctr 最高的前{TOPK}名關鍵字": self.top_k_ctr,
            f"透過 google 搜尋點進 PyConTW 官網的所有關鍵字中，官網排名位置越靠前的前{TOPK}名": self.top_k_position,
            f"clicks 最高的前{TOPK}名關鍵字": self.top_k_clicks,
            f"impressions 最高的前{TOPK}名關鍵字": self.top_k_impressions,
        }

        for msg, heap in msg_heap_dict.items():
            self._send_msg_to_discord(msg, heap)
        self._send_team_msg()

    def _maitain_topk_heap(self, report_msg):
        def heappush(heap, item, topk):
            heapq.heappush(heap, item)
            while len(heap) > topk:
                heapq.heappop(heap)

        for row in report_msg.rows:
            heappush(self.top_k_ctr, (row.ctr, row.query), TOPK)
            heappush(self.top_k_position, (-row.position, row.query), TOPK)
            heappush(self.top_k_clicks, (row.clicks, row.query), TOPK)
            heappush(self.top_k_impressions, (row.impressions, row.query), TOPK)

    @staticmethod
    def _send_msg_to_discord(msg, heap):
        def get_topk_from_heap(heap):
            def turn_negative_back_to_positive_int(heap):
                return [(num if num >= 0 else -num, query) for num, query in heap]

            return turn_negative_back_to_positive_int(sorted(heap, key=lambda x: -x[0]))

        def format_heap_content(topk_heap):
            return "\n".join([f'"{query}"\t{num}' for num, query in topk_heap])

        topk_heap = get_topk_from_heap(heap)
        formatted_heap_content = format_heap_content(topk_heap)
        requests.post(
            DISCORD_WEBHOOK,
            json={
                "username": "Data Team 雙週報",
                "content": f"{msg}：\n {formatted_heap_content}\n----------------------\n",
            },
        )

    @staticmethod
    def _send_team_msg():
        requests.post(
            DISCORD_WEBHOOK,
            json={
                "username": "Data Team 雙週報",
                "content": "有任何問題，歡迎敲 data team 任何一位成員~",
            },
        )


if __name__ == "__main__":
    g = GoogleSearchConsoleReporter()
    g.main()
