import pandas as pd
import requests
from io import StringIO         
import time
import tqdm
from pathlib import Path

def get_horse_results_ajax(horse_id: str) -> pd.DataFrame:
    """
    netkeibaのAJAXエンドポイントから返されるHTMLを、正しい文字コードで解析する。
    """
    ajax_url = 'https://db.netkeiba.com/horse/ajax_horse_results.html'
    params = {'id': horse_id}
    headers = {
        "Referer": f"https://db.netkeiba.com/horse/{horse_id}/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        response = requests.get(ajax_url, params=params, headers=headers)
        response.raise_for_status()
        response.encoding = 'EUC-JP'
        html_table = response.text
        df_list = pd.read_html(StringIO(html_table))
            
        if df_list:
            race_results_df = df_list[0]
            race_results_df['horse_id'] = horse_id
            return race_results_df
        
        return None
            
    except requests.exceptions.RequestException as e:
        print(f"馬ID {horse_id} のデータ取得中にHTTPエラー: {e}")
        return None
    except Exception as e:
        print(f"馬ID {horse_id} のデータ取得中に予期せぬエラー: {e}")
        return None

# --- ここからが実行部分 ---
if __name__ == '__main__':
    horse_ids_to_scrape = [
        '2022105102',
        '2019106214',
        '2020104242',
    ]
    
    saved_count = 0
    
    columns_to_drop = [
        '映像',
        '馬場指数',
        'ﾀｲﾑ指数',
        '厩舎ｺﾒﾝﾄ',
        '備考',
        '水分量',
        '賞金'
    ]

    print(f"{len(horse_ids_to_scrape)}頭の馬のデータを取得し、不要な列を削除して個別に保存します...")
    
    for horse_id in tqdm(horse_ids_to_scrape):
        results_df = get_horse_results_ajax(horse_id)
        
        if results_df is not None:
            
            # --- ▼▼▼ 変更点2：列を削除する処理を追加 ▼▼▼ ---
            # errors='ignore' は、リスト内の列名が存在しなくてもエラーにしないためのオプション
            results_df = results_df.drop(columns=columns_to_drop, errors='ignore')
            # --- ▲▲▲ 変更点2 ▲▲▲ ---

            # 1. 保存先のフォルダパスを作成
            # フォルダ名を data/horse から data に変更
            target_dir = Path("data/horse") / horse_id
            
            # 2. フォルダが存在しない場合に、フォルダを作成
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # 3. 保存するファイル名を定義
            output_path = target_dir / f"{horse_id}_race_results.csv"

            # 4. CSVファイルとして保存
            results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            saved_count += 1
        
        # time.sleep(1) # sleep時間を1秒に戻すことを推奨します

    print(f"\n処理が完了しました。")
    print(f"{len(horse_ids_to_scrape)}頭中、{saved_count}頭分のデータを正常に保存しました。")
    print(f"データは 'data' フォルダ内に馬IDごとのフォルダに格納されています。")