import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import time
from tqdm import tqdm
from pathlib import Path

def get_race_results(race_id: str) -> pd.DataFrame:
    """
    指定されたrace_idのnetkeiba.comのページから、
    そのレースの全出走馬の結果テーブルを取得する。
    """
    url = f'https://db.netkeiba.com/race/{race_id}/'
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # ★重要: レースページの静的HTMLも文字コードは 'EUC-JP'
        response.encoding = 'EUC-JP'
        html_content = response.text

        # BeautifulSoupでHTMLを解析
        soup = BeautifulSoup(html_content, "lxml")
        
        # 目的のテーブルをclass属性で特定
        # レース結果のテーブルは 'race_table_01' というクラスを持っている
        race_table = soup.find("table", class_="race_table_01")
        
        if race_table:
            # 見つけたテーブルを文字列に変換し、pandasで読み込む
            df_list = pd.read_html(StringIO(str(race_table)))
            results_df = df_list[0]
            
            # どのレースのデータか分かるように、race_idの列を追加
            results_df['race_id'] = race_id
            return results_df
        else:
            print(f"エラー: race_id '{race_id}' のページでレース結果テーブルが見つかりませんでした。")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"race_id {race_id} のデータ取得中にHTTPエラー: {e}")
        return None
    except Exception as e:
        print(f"race_id {race_id} のデータ取得中に予期せぬエラー: {e}")
        return None

# --- ここからが実行部分 ---
if __name__ == '__main__':
    # 例として、クロワデュノールが出走した皐月賞(GI)のID
    test_race_id = '202506030811'
    
    # 複数のIDをリストで管理する場合
    race_ids_to_scrape = [
        '202506030811', # 皐月賞(GI)
        '202505021211', # 東京優駿(GI)
        '202406050911', # ホープフルS(GI)
    ]
    
    print(f"{len(race_ids_to_scrape)} 件のレースデータを取得します...")

    # 保存用のベースフォルダを定義
    # data/race/ というフォルダに保存する
    base_dir = Path("data") / "race"
    base_dir.mkdir(parents=True, exist_ok=True)
    
    saved_count = 0

    for race_id in tqdm(race_ids_to_scrape):
        results_df = get_race_results(race_id)
        
        if results_df is not None:
            # 保存するファイルパスを定義
            output_path = base_dir / f"race_{race_id}_results.csv"
            
            # CSVファイルとして保存
            results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            saved_count += 1
        
        # サーバー負荷軽減のための待機
        time.sleep(1)

    print(f"\n処理が完了しました。")
    print(f"{len(race_ids_to_scrape)}件中、{saved_count}件のデータを正常に保存しました。")
    print(f"データは '{base_dir}' フォルダ内に格納されています。")