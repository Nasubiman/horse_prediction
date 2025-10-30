import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import time
from tqdm import tqdm
from pathlib import Path
import re 

def get_race_results_with_ids(race_id: str) -> pd.DataFrame:
    """
    【エラー修正版】
    指定されたrace_idのページを詳細に解析し、
    テキストデータと同時に、リンクから horse_id や jockey_id も抽出する。
    <tbody> がなくても動作するように修正済み。
    """
    url = f'https://db.netkeiba.com/race/{race_id}/'

#     USER_AGENTS = [
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
#     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
# ]

#     # リクエストのたびに、リストからランダムに1つ選ぶ
#     random_ua = random.choice(USER_AGENTS)

#     headers = {
#         "User-Agent": random_ua
#     }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response.encoding = 'EUC-JP'
        html_content = response.text

        soup = BeautifulSoup(html_content, "lxml")
        
        race_table = soup.find("table", class_="race_table_01")
        
        if not race_table:
            # 存在しないレースIDの場合、テーブルが見つからないのでNoneを返す (正常)
            return None
        
        all_rows_data = []
        
        # <tbody> なしでも動作するよう、tableから直接 tr を探す
        for row in race_table.find_all('tr'):
            cols = row.find_all('td')
            if not cols or len(cols) < 16: # ヘッダー行やデータ不足行をスキップ
                continue 

            horse_id_pattern = re.compile(r'/horse/(\d{10})')
            jockey_id_pattern = re.compile(r'/jockey/result/recent/([\w\d]+)/')
            trainer_id_pattern = re.compile(r'/trainer/result/recent/([\w\d]+)/')

            horse_id, jockey_id, trainer_id = None, None, None

            horse_link = cols[3].find('a', href=horse_id_pattern)
            if horse_link:
                match = horse_id_pattern.search(horse_link['href'])
                if match: horse_id = match.group(1)

            jockey_link = cols[6].find('a', href=jockey_id_pattern)
            if jockey_link:
                match = jockey_id_pattern.search(jockey_link['href'])
                if match: jockey_id = match.group(1)

            trainer_link = cols[14].find('a', href=trainer_id_pattern)
            if trainer_link:
                match = trainer_id_pattern.search(trainer_link['href'])
                if match: trainer_id = match.group(1)

            row_data = {
                '着 順': cols[0].get_text(strip=True),
                '枠 番': cols[1].get_text(strip=True),
                '馬 番': cols[2].get_text(strip=True),
                '馬名': cols[3].get_text(strip=True),
                '性齢': cols[4].get_text(strip=True),
                '斤量': cols[5].get_text(strip=True),
                '騎手': cols[6].get_text(strip=True),
                'タイム': cols[7].get_text(strip=True),
                '着差': cols[8].get_text(strip=True),
                '単勝': cols[12].get_text(strip=True),
                '人 気': cols[13].get_text(strip=True),
                '馬体重': cols[14].get_text(strip=True),
                '調教師': cols[15].get_text(strip=True),
                'race_id': race_id,
                'horse_id': horse_id,
                'jockey_id': jockey_id,
                'trainer_id': trainer_id,
            }
            all_rows_data.append(row_data)

        if not all_rows_data:
            return None
            
        return pd.DataFrame(all_rows_data)
            
    except requests.exceptions.RequestException as e:
        # 404 (Not Found) エラーもここでキャッチされる
        # 存在しないIDなので、Noneを返してスキップする (正常)
        return None
    except Exception as e:
        print(f"race_id {race_id} のデータ取得中に予期せぬエラー: {e}")
        return None

# --- ▼▼▼ ここからが実行部分 (総当たりロジック) ▼▼▼ ---
if __name__ == '__main__':
    
    # --- 変更点 (ここから) ---
    YEARS_TO_SCRAPE = range(2000, 2002) 
    # --- 変更点 (ここまで) ---

    # JRA競馬場コード (01〜10)
    TRACK_CODES = [f"{i:02d}" for i in range(1, 11)]
    # 開催回 (01〜05) - 5回開催は稀だが、安全マージン
    SESSION_CODES = [f"{i:02d}" for i in range(1, 6)]
    # 開催日 (01〜12) - 12日開催は稀だが、安全マージン
    DAY_CODES = [f"{i:02d}" for i in range(1, 13)]
    # レース番号 (01〜12)
    RACE_NUMBERS = [f"{i:02d}" for i in range(1, 13)]

    # 全試行回数 (1年あたり)
    total_attempts_per_year = len(TRACK_CODES) * len(SESSION_CODES) * len(DAY_CODES) * len(RACE_NUMBERS)
    
    # --- 変更点 (ここから) ---
    # 年ごとのループを追加
    for year_int in YEARS_TO_SCRAPE:
        YEAR = str(year_int)
    # --- 変更点 (ここまで) ---
    
        print("-" * 50)
        print(f"{YEAR}年のレースデータを総当たりで取得します。")
        print(f"競馬場: {len(TRACK_CODES)}件 / 開催回: {len(SESSION_CODES)}件 / 開催日: {len(DAY_CODES)}件 / レース: {len(RACE_NUMBERS)}件")
        print(f"合計 {total_attempts_per_year} パターンのIDを試行します。")
        print(f"推定所要時間: 約 {total_attempts_per_year / 3600:.1f} 時間 (1秒/件 の場合)")
        
        # 保存先のベースフォルダを指定
        base_dir = Path("data/race") / YEAR
        base_dir.mkdir(parents=True, exist_ok=True)
        
        saved_count = 0
        
        # tqdmでネストされたループの進捗を表示
        with tqdm(total=total_attempts_per_year, desc=f"{YEAR}年") as pbar:
            for track in TRACK_CODES:
                for session in SESSION_CODES:
                    for day in DAY_CODES:
                        for race_num in RACE_NUMBERS:
                            
                            race_id = f"{YEAR}{track}{session}{day}{race_num}"
                            pbar.set_description(f"Check: {race_id}")
                            
                            results_df = get_race_results_with_ids(race_id)
                            
                            # データを取得できた (レースが存在した) 場合のみ保存
                            if results_df is not None:
                                output_path = base_dir / f"race_{race_id}_results.csv"
                                results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                                saved_count += 1
                            
                            # 1秒待機 (必須)
                            time.sleep(1.2)
                            pbar.update(1) # プログレスバーを1進める

        print(f"\n{YEAR}年の処理が完了しました。")
        print(f"{total_attempts_per_year}件のIDを試行し、{saved_count}件のレースデータを正常に保存しました。")
        print(f"データは '{base_dir}' フォルダ内に格納されています。")

    print("-" * 50)
    print("指定された全ての年の処理が完了しました。")