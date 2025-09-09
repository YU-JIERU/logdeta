import streamlit as st
import pandas as pd
import io
import time

# 年2桁→4桁変換
def convert_short_year_to_full(date_str: str) -> str:
    parts = date_str.split("/")
    if len(parts) == 3 and len(parts[0]) == 2:
        year = int(parts[0])
        year += 2000 if year < 70 else 1900
        return f"{year}/{parts[1]}/{parts[2]}"
    return date_str

# CSV読み込みと前処理
def load_csv(uploaded_file: io.BytesIO) -> pd.DataFrame:
    try:
        df = pd.read_csv(uploaded_file, dtype=str, encoding='utf-8', engine='pyarrow')
    except Exception:
        uploaded_file.seek(0)
        try:
            df = pd.read_csv(uploaded_file, dtype=str, encoding='shift_jis', engine='c')
        except Exception as e:
            st.warning(f"{uploaded_file.name} の読み込みに失敗しました: {e}")
            return pd.DataFrame()

    df.columns = df.columns.str.strip().str.replace(r"\s+", "", regex=True).str.replace('　', '')
    rename_map = {
        col: '日付' if '日付' in col or col.lower() in ['date', 'day']
        else '時刻' if '時刻' in col or col.lower() == 'time'
        else col for col in df.columns
    }
    df.rename(columns=rename_map, inplace=True)

    if '日付' not in df.columns or '時刻' not in df.columns:
        st.warning(f"{uploaded_file.name} に '日付' または '時刻' 列が見つかりません。")
        return pd.DataFrame()

    # 2重ヘッダー対応 + 「資料」などのラベル行を除外
    df = df[~df['日付'].str.contains('日付', na=False) & ~df['時刻'].str.contains('時刻', na=False)]
    df = df[~df['日付'].str.contains('資料', na=False)]  # ← 追加：資料行を除外

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

# マージ＆並び替え
def merge_and_sort(dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [df for df in dataframes if not df.empty]
    if not non_empty:
        return pd.DataFrame()

    merged = pd.concat(non_empty, ignore_index=True).drop_duplicates().sort_values('datetime').reset_index(drop=True)

    if 'datetime' in merged.columns and '時刻' in merged.columns:
        cols = list(merged.columns)
        cols.remove('datetime')
        time_idx = cols.index('時刻') + 1
        cols.insert(time_idx, 'datetime')
        merged = merged[cols]

    return merged

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

        temp_dfs = []
        start_time = time.time()
        total_files = len(uploaded_files)

        # 読み込み処理
        for idx, file in enumerate(uploaded_files):
            df = load_csv(file)
            temp_dfs.append(df)

            progress_percent = int((idx + 1) / total_files * 30)
            progress.progress(progress_percent)

            status_text.text(
                f"📄 ファイル読み込み中: {idx + 1}/{total_files} - {file.name} ({len(df)} 行)"
            )

        read_time = time.time() - start_time

        if not any(not df.empty for df in temp_dfs):
            st.warning('有効なデータがありません')
            st.stop()

        # 間引き処理
        start_time = time.time()
        reduced_dfs = []
        for idx, df in enumerate(temp_dfs):
            reduced_df = filter_by_interval(df, interval_seconds)
            reduced_dfs.append(reduced_df)

            progress_percent = 30 + int((idx + 1) / len(temp_dfs) * 40)
            progress.progress(progress_percent)

            status_text.text(
                f"🔧 間引き処理中: {idx + 1}/{len(temp_dfs)} ファイル目"
            )

        filter_time = time.time() - start_time

        # 結合処理
        start_time = time.time()
        merged_df = merge_and_sort(reduced_dfs)
        merge_time = time.time() - start_time

        progress.progress(100)
        status_text.text('✅ 完了！')

        st.success(
            f"処理完了！ 合計 {len(merged_df)} 件を抽出しました\n"
            f"読み込み時間: {read_time:.2f}秒 | 間引き時間: {filter_time:.2f}秒 | 結合時間: {merge_time:.2f}秒"
        )

        csv_bytes = generate_csv(merged_df)
        st.download_button('📥 ダウンロードはこちら', csv_bytes, file_name='filtered_interval_data.csv', mime='text/csv')

# アプリ実行
if __name__ == '__main__':
    main()
