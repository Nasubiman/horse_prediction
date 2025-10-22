import requests
from bs4 import BeautifulSoup
import time
import re
from tqdm import tqdm

def get_horse_ids_for_year(year: int = 2002) -> list:
    """
    指定された年に生まれた馬のIDをnetkeibaの検索結果から全て収集する。
    """
    print(f"{year}年生まれの馬IDの収集を開始します...")
    horse_ids = set() # 重複を避けるためにセットを使用
    page = 1
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    }
    
    # /horse/2002... のようなURLから10桁のIDを抽出する正規表現
    horse_id_pattern = re.compile(r'/horse/(\d{10})')

    while True:
        # 2002年生まれの馬を100件ずつ表示する検索結果ページのURL
        # list=100 で100件表示（最大値）
        list_url = f"https://db.netkeiba.com/?pid=horse_search_list&sort_key=birth_year&sort_type=desc&year_from={year}&year_to={year}&list=100&page={page}"
        
        print(f"\n{page}ページ目をスクレイピング中...\nURL: {list_url}")
        
        try:
            response = requests.get(list_url, headers=headers)
            response.raise_for_status()
            response.encoding = 'EUC-JP'
            
            soup = BeautifulSoup(response.text, "lxml")
            
            # ページ内の馬へのリンクを全て探す
            links = soup.find_all('a', href=horse_id_pattern)
            
            if not links:
                print("このページに馬のリンクが見つかりませんでした。収集を終了します。")
                break
                
            found_on_page = 0
            for link in links:
                match = horse_id_pattern.search(link['href'])
                if match:
                    horse_id = match.group(1)
                    # IDが指定された年で始まることを確認
                    if horse_id.startswith(str(year)):
                        if horse_id not in horse_ids:
                            horse_ids.add(horse_id)
                            found_on_page += 1
            
            if found_on_page == 0:
                print("このページに該当する馬のリンクは見つかりましたが、新規IDはありませんでした。収集を終了します。")
                break

            print(f"  -> {found_on_page}件の新規IDを発見。 (現在までの合計: {len(horse_ids)})")
            page += 1
            time.sleep(1) # サーバー負荷軽減のため1秒待機

        except requests.exceptions.RequestException as e:
            print(f"エラーが発生しました: {e}")
            break
        except Exception as e:
            print(f"予期せぬエラー: {e}")
            break
            
    return sorted(list(horse_ids))

if __name__ == '__main__':
    # 2002年生まれのIDを取得
    ids_2002 = get_horse_ids_for_year(2002)
    
    if ids_2002:
        output_file = 'horse_ids_2002.txt'
        with open(output_file, 'w') as f:
            for horse_id in ids_2002:
                f.write(f"{horse_id}\n")
        print(f"\n処理完了。合計 {len(ids_2002)} 件の馬IDを {output_file} に保存しました。")
    else:
        print("\n馬IDを1件も取得できませんでした。")