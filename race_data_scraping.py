import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import time
from tqdm import tqdm
from pathlib import Path
import re # ★ 正規表現ライブラリをインポート

def get_race_results_with_ids(race_id: str) -> pd.DataFrame:
    """
    【改良版】
    指定されたrace_idのページを詳細に解析し、
    テキストデータと同時に、リンクから horse_id や jockey_id も抽出する。
    """
    url = f'https://db.netkeiba.com/race/{race_id}/'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response.encoding = 'EUC-JP'
        html_content = response.text

        soup = BeautifulSoup(html_content, "lxml")
        
        race_table = soup.find("table", class_="race_table_01")
        
        if not race_table:
            print(f"エラー: race_id '{race_id}' でテーブルが見つかりません。")
            return None
        
        # --- ここからが詳細な解析処理 ---
        
        # 抽出した全行のデータを格納するリスト
        all_rows_data = []
        
        # テーブルのボディ(tbody)から全行(tr)を取得
        for row in race_table.find('tbody').find_all('tr'):
            cols = row.find_all('td')
            if not cols:
                continue # ヘッダー行などをスキップ

            # リンクからIDを抽出する正規表現
            horse_id_pattern = re.compile(r'/horse/(\d{10})')
            jockey_id_pattern = re.compile(r'/jockey/result/recent/([\w\d]+)/')
            trainer_id_pattern = re.compile(r'/trainer/result/recent/([\w\d]+)/')

            horse_id, jockey_id, trainer_id = None, None, None

            # 馬名 (3番目のセル) から horse_id を抽出
            horse_link = cols[3].find('a', href=horse_id_pattern)
            if horse_link:
                match = horse_id_pattern.search(horse_link['href'])
                if match:
                    horse_id = match.group(1)

            # 騎手 (6番目のセル) から jockey_id を抽出
            jockey_link = cols[6].find('a', href=jockey_id_pattern)
            if jockey_link:
                match = jockey_id_pattern.search(jockey_link['href'])
                if match:
                    jockey_id = match.group(1)

            # 調教師 (14番目のセル) から trainer_id を抽出
            trainer_link = cols[14].find('a', href=trainer_id_pattern)
            if trainer_link:
                match = trainer_id_pattern.search(trainer_link['href'])
                if match:
                    trainer_id = match.group(1)

            # データを辞書として格納
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
                # 9: タイム指数, 10: 通過, 11: 上り
                '単勝': cols[12].get_text(strip=True),
                '人 気': cols[13].get_text(strip=True),
                '馬体重': cols[14].get_text(strip=True),
                '調教師': cols[15].get_text(strip=True), # 調教師名は15番目
                'race_id': race_id,
                'horse_id': horse_id,
                'jockey_id': jockey_id,
                'trainer_id': trainer_id,
            }
            all_rows_data.append(row_data)

        if not all_rows_data:
            return None
            
        # 辞書のリストからDataFrameを作成
        return pd.DataFrame(all_rows_data)
            
    except requests.exceptions.RequestException as e:
        print(f"race_id {race_id} のデータ取得中にHTTPエラー: {e}")
        return None
    except Exception as e:
        print(f"race_id {race_id} のデータ取得中に予期せぬエラー: {e}")
        return None

# --- 実行部分 (使用例) ---
if __name__ == '__main__':
    
    race_ids_to_scrape = [
        '202406050911', # ユーザーが例示したホープフルS
        '202505021211', # 東京優駿(GI)
    ]
    
    print(f"{len(race_ids_to_scrape)} 件のレースデータを詳細解析します...")

    base_dir = Path("data") / "race_with_ids" # 保存先フォルダを別名に変更
    base_dir.mkdir(parents=True, exist_ok=True)
    
    saved_count = 0

    for race_id in tqdm(race_ids_to_scrape):
        # ★改良版の関数を呼び出す
        results_df = get_race_results_with_ids(race_id) 
        
        if results_df is not None:
            output_path = base_dir / f"race_{race_id}_results.csv"
            results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            saved_count += 1
        
        time.sleep(1)

    print(f"\n処理が完了しました。")
    print(f"{len(race_ids_to_scrape)}件中、{saved_count}件のデータを正常に保存しました。")
    print(f"データは '{base_dir}' フォルダ内に格納されています。")