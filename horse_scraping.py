import pandas as pd
import requests
from io import StringIO
import time
from tqdm import tqdm # tqdmのimportを修正
from pathlib import Path

def get_horse_results_ajax(horse_id: str) -> pd.DataFrame:
    """
    netkeibaのAJAXエンドポイントから返されるHTMLを、正しい文字コードで解析する。
    この関数は変更ありません。
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
        
        # 存在しないIDの場合、df_listが空になり、ここに来る (仕様)
        return None
            
    except requests.exceptions.RequestException as e:
        # 存在しないIDにアクセスすると404エラーになるが、
        # response.raise_for_status()がそれを検知するので、
        # ここでNoneを返して「存在しなかった」と扱う
        return None
    except Exception as e:
        # print(f"馬ID {horse_id} のデータ取得中に予期せぬエラー: {e}")
        return None

# --- ▼▼▼ ここからが実行部分 (変更点) ▼▼▼ ---
if __name__ == '__main__':
    
    # 2002年生まれと仮定されるIDの範囲を定義
    start_id = 2002100001
    end_id   = 2002109999 # このIDまで試行 (合計9,999件)
    
    saved_count = 0
    total_attempts = (end_id - start_id) + 1
    
    columns_to_drop = [
        '映像',
        '馬場指数',
        'ﾀｲﾑ指数',
        '厩舎ｺﾒﾝﾄ',
        '備考',
        '水分量',
        '賞金'
    ]

    print(f"2002年産駒の馬ID {start_id} から {end_id} までの総当たり取得を開始します...")
    print(f"合計 {total_attempts} 件のIDを試行します。")
    
    # tqdmを使って、range()で生成されるIDのループにプログレスバーを表示
    for horse_id_num in tqdm(range(start_id, end_id + 1)):
        
        horse_id = str(horse_id_num) # 数値を文字列のIDに変換
        
        results_df = get_horse_results_ajax(horse_id)
        
        # results_dfがNoneではない (＝馬が存在した) 場合のみ保存処理
        if results_df is not None:
            
            # 不要な列を削除
            results_df = results_df.drop(columns=columns_to_drop, errors='ignore')

            # 保存先フォルダ (data/horse/2002100570/ など)
            target_dir = Path("data/horse") / horse_id
            target_dir.mkdir(parents=True, exist_ok=True)
            
            output_path = target_dir / f"{horse_id}_race_results.csv"

            # CSVファイルとして保存
            results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            saved_count += 1
        
        # サーバー負荷軽減とIPブロック回避のため、1秒待機 (必須)
        # time.sleep(1)

    print(f"\n処理が完了しました。")
    print(f"合計 {total_attempts} 件のIDを試行し、{saved_count} 頭分のデータを正常に保存しました。")
    print(f"データは 'data/horse' フォルダ内に馬IDごとのフォルダに格納されています。")