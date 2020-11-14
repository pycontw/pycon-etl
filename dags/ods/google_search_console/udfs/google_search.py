import heapq

import requests
import searchconsole

from dags.config import DISCORD_WEBHOOK

TOPK = 5


class GoogleSearchConsoleReporter(object):
    def __init__(self):
        def get_report():
            account = searchconsole.authenticate(
                client_config="client_secret_google_search_console.json",
                credentials="client_secret_google_search_console_serialized.json",
            )
            webproperty = account["https://tw.pycon.org/"]
            return webproperty.query.range("today", days=-7).dimension("query").get()

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
        self.report = get_report()

    def send_report(self):
        self.maitain_topk_heap()
        msg_heap_dict = {
            f"透過 google 搜尋點進 PyConTW 官網的所有關鍵字，ctr 最高的前{TOPK}名關鍵字": self.top_k_ctr,
            f"透過 google 搜尋點進 PyConTW 官網的所有關鍵字中，官網排名位置越靠前的前{TOPK}名": self.top_k_position,
            f"clicks 最高的前{TOPK}名關鍵字": self.top_k_clicks,
            f"impressions 最高的前{TOPK}名關鍵字": self.top_k_impressions,
        }

        for msg, heap in msg_heap_dict.items():
            self.send_msg_to_discord(msg, heap)
        self.send_team_msg()

    def maitain_topk_heap(self):
        def heappush(heap, item, topk):
            heapq.heappush(heap, item)
            while len(heap) > topk:
                heapq.heappop(heap)

        for row in self.report.rows:
            heappush(self.top_k_ctr, (row.ctr, row.query), TOPK)
            heappush(self.top_k_position, (-row.position, row.query), TOPK)
            heappush(self.top_k_clicks, (row.clicks, row.query), TOPK)
            heappush(self.top_k_impressions, (row.impressions, row.query), TOPK)

    @staticmethod
    def send_msg_to_discord(msg, heap):
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
                "username": "Data Team 週報",
                "content": f"{msg}：\n {formatted_heap_content}\n----------------------\n",
            },
        )

    @staticmethod
    def send_team_msg():
        requests.post(
            DISCORD_WEBHOOK,
            json={"username": "Data Team 週報", "content": "有任何問題，歡迎敲 data team 任何一位成員~"},
        )


if __name__ == "__main__":
    g = GoogleSearchConsoleReporter()
    g.send_report()
