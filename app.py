import streamlit as st
import pandas as pd
import io
import time
import unicodedata
import re
import gc

# 行数制限（1ファイルあたり）
MAX_ROWS_PER_FILE = 10000

# 年2桁→4桁変換
def convert_short_year_to_full(date_str: str) -> str:
    parts = date_str.split("/")
    if len(parts) == 3 and len(parts[0]) == 2:
        year = int(parts[0])
        year += 2000 if year < 70 else 1900
        return f"{year}/{parts[1]}/{parts[2]}"
    return date_str

# カラム名の正規化（丸数字・全角・空白など削除）
def normalize_column_name(col_name: str) -> str:
    col_name = unicodedata.normalize('NFKC', col_name)
    col_name = re.sub(r'[\s　\t\r\n①-⑳㉑-㉟⑴-⒇⓪-⓿①-⓾①-⑩]', '', col_name)
    return col_name.lower()

# CSV読み込みと前処理（エンコーディング自動判定付き）
def load_csv(uploaded_file: io.BytesIO) -> pd.DataFrame:
    encodings_to_try = ['utf-8', 'cp932', 'shift_jis', 'utf-16', 'utf-8-sig', 'latin1']
    df = None
    for encoding in encodings_to_try:
        uploaded_file.seek(0)
        try:
            df = pd.read_csv(uploaded_file, dtype=str, encoding=encoding, engine='python')
            break
        except Exception:
            continue

    if df is None:
        st.warning(f"{uploaded_file.name} の読み込みに失敗しました（対応できる文字コードが見つかりませんでした）")
        return pd.DataFrame()

    # カラム名の正規化とマッピング
    normalized_columns = {col: normalize_column_name(col) for col in df.columns}
    rename_map = {
        col: '日付' if '日付' in norm or 'date' in norm or 'day' in norm
        else '時刻' if '時刻' in norm or 'time' == norm
        else col
        for col, norm in normalized_columns.items()
    }
    df.rename(columns=rename_map, inplace=True)

    if '日付' not in df.columns or '時刻' not in df.columns:
        st.warning(f"{uploaded_file.name} に '日付' または '時刻' 列が見つかりません。")
        return pd.DataFrame()

    # 2重ヘッダー対応
    df = df[~df['日付'].str.contains('日付', na=False) & ~df['時刻'].str.contains('時刻', na=False)]

    df['日付'] = df['日付'].astype(str).str.replace(r'[\s　\t\r\n]+', '', regex=True)
    df['時刻'] = df['時刻'].astype(str).str.replace(r'[\s　\t\r\n]+', '', regex=True)
    df = df[(df['日付'] != '') & (df['時刻'] != '')]
    if df.empty:
        st.warning(f"{uploaded_file.name} に有効な行がありません。")
        return pd.DataFrame()

    df['日付'] = df['日付'].apply(convert_short_year_to_full)

    try:
        df['日付'] = pd.to_datetime(df['日付'], format='%Y/%m/%d').dt.strftime('%Y-%m-%d')
    except Exception as e:
        st.warning(f"{uploaded_file.name} の日付変換に失敗しました: {e}")
        return pd.DataFrame()

    try:
        df['時刻'] = pd.to_datetime(df['時刻'], errors='coerce').dt.strftime('%H:%M:%S')
    except Exception as e:
        st.warning(f"{uploaded_file.name} の時刻変換に失敗しました: {e}")
        return pd.DataFrame()

    dt_str = df['日付'] + ' ' + df['時刻']
    df['datetime'] = pd.to_datetime(dt_str, errors='coerce')
    df.dropna(subset=['datetime'], inplace=True)

    # 行数制限
    if len(df) > MAX_ROWS_PER_FILE:
        st.info(f"{uploaded_file.name} は {MAX_ROWS_PER_FILE} 行までに制限されます。")
        df = df.iloc[:MAX_ROWS_PER_FILE]

    return df.reset_index(drop=True)

# 間引き処理
def filter_by_interval(df: pd.DataFrame, interval_seconds: int) -> pd.DataFrame:
    if 'datetime' not in df.columns:
        st.error("datetime列もしくは日付・時刻列がありません")
        st.stop()

    if interval_seconds <= 0:
        return df.reset_index(drop=True)

    df['datetime_rounded'] = df['datetime'].dt.floor(f'{interval_seconds}S')
    reduced = df.groupby('datetime_rounded').first().copy()
    reduced.rename(columns={'datetime_rounded': 'datetime'}, inplace=True)
    reduced.reset_index(inplace=True)

    important_cols = [col for col in ['循環水流量'] if col in reduced.columns]
    if important_cols:
        reduced = reduced.dropna(subset=important_cols)

    return reduced.reset_index(drop=True)

# CSVダウンロード用生成
def generate_csv(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding='utf-8-sig')
    return buf.getvalue().encode('utf-8-sig')

# 抽出間隔セレクタ
def select_interval() -> int:
    options = ['全件抽出(0)', '5秒', '10秒', '30秒', '1分', '5分', '10分']
    seconds_map = {
        '全件抽出(0)': 0,
        '5秒': 5,
        '10秒': 10,
        '30秒': 30,
        '1分': 60,
        '5分': 300,
        '10分': 600
    }
    option = st.selectbox('抽出間隔を選択', options, index=4)
    return seconds_map[option]

# キャッシュ付きファイル読み込み関数
@st.cache_data(show_spinner=False)
def load_and_process_file(file: io.BytesIO) -> pd.DataFrame:
    return load_csv(file)

# アプリ本体
def main():
    st.set_page_config(page_title='ログ整形 ᔦ--ᔨ', layout='centered', initial_sidebar_state='expanded')
    st.title('ログ整形 ᔦꙬᔨ')

    interval_seconds = select_interval()
    uploaded_files = st.file_uploader('CSVファイルをアップロードしてください', type=['csv'], accept_multiple_files=True)
    start_clicked = st.button('▶ スタート')

    if start_clicked:
        if not uploaded_files:
            st.warning('CSVファイルをアップロードしてください')
            st.stop()

        progress = st.progress(0)
        status_text = st.empty()
        total_files = len(uploaded_files)

        merged_df = pd.DataFrame()

        start_time = time.time()
        for idx, file in enumerate(uploaded_files):
            # 読み込み＋間引き
            df = load_and_process_file(file)
            reduced_df = filter_by_interval(df, interval_seconds)

            # マージ
            if not reduced_df.empty:
                merged_df = pd.concat([merged_df, reduced_df], ignore_index=True)

            # メモリ解放
            del df, reduced_df
            gc.collect()

            # 進捗表示（軽量化）
            if (idx + 1) % 3 == 0 or idx == total_files - 1:
                progress_percent = int((idx + 1) / total_files * 90)
                progress.progress(progress_percent)

            status_text.text(f"📄 処理中: {idx + 1}/{total_files} - {file.name}")

        total_time = time.time() - start_time

        if merged_df.empty:
            st.warning("有効なデータがありませんでした。")
            st.stop()

        # 並び替え（最後だけ）
        merged_df = merged_df.drop_duplicates().sort_values('datetime').reset_index(drop=True)

        progress.progress(100)
        status_text.text("✅ 完了！")

        st.success(
            f"処理完了！ 合計 {len(merged_df)} 件を抽出しました\n"
            f"処理時間: {total_time:.2f} 秒"
        )

        csv_bytes = generate_csv(merged_df)
        st.download_button('📥 ダウンロードはこちら', csv_bytes, file_name='filtered_interval_data.csv', mime='text/csv')


if __name__ == '__main__':
    main()
