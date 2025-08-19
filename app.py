import streamlit as st
import pandas as pd
import io
import time

def load_csv(uploaded_file: io.BytesIO) -> pd.DataFrame:
    try:
        df = pd.read_csv(uploaded_file, dtype=str, encoding="utf-8", sep=",", engine="c")
    except UnicodeDecodeError:
        uploaded_file.seek(0)
        df = pd.read_csv(uploaded_file, dtype=str, encoding="shift_jis", sep=",", engine="c")
    df.columns = df.columns.str.strip().str.replace(r"\s+", "", regex=True)
    rename_map = {}
    for col in df.columns:
        if "日付" in col or col.lower() in ["date", "day"]:
            rename_map[col] = "日付"
        if "時刻" in col or col.lower() in ["time"]:
            rename_map[col] = "時刻"
    df.rename(columns=rename_map, inplace=True)
    if "日付" not in df.columns or "時刻" not in df.columns:
        return pd.DataFrame()
    dt_str = df["日付"].astype(str).str.strip() + " " + df["時刻"].astype(str).str.strip()
    df["datetime"] = pd.to_datetime(dt_str, errors="coerce")
    df.dropna(subset=["datetime"], inplace=True)
    return df.reset_index(drop=True)

def filter_by_interval(df: pd.DataFrame, interval_seconds: int, base_time=None) -> pd.DataFrame:
    if interval_seconds <= 0:
        return df.reset_index(drop=True)
    if base_time is None:
        base_time = df["datetime"].min()
    df_filtered = (
        df.groupby(((df["datetime"] - base_time).dt.total_seconds() // interval_seconds).astype(int))
          .first()
          .reset_index(drop=True)
    )
    return df_filtered

def merge_and_sort(dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    try:
        trimmed_dfs = [df.drop_duplicates(subset=["datetime"]) for df in dataframes]
        merged = pd.concat(trimmed_dfs, ignore_index=True)
        merged = merged.drop_duplicates(subset=["datetime"])
        merged = merged.sort_values("datetime").reset_index(drop=True)
        return merged
    except Exception as e:
        st.error(f"結合処理中にエラーが発生しました: {e}")
        return pd.DataFrame()

def generate_csv(df: pd.DataFrame) -> bytes:
    if "datetime" in df.columns:
        df = df.drop(columns=["datetime"])
    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    return buf.getvalue().encode("utf-8-sig")

def select_interval() -> int:
    interval_option = st.selectbox(
        "抽出間隔を選択",
        options=[
            ("全件抽出(0)", 0),
            ("5秒", 5),
            ("10秒", 10),
            ("30秒", 30),
            ("1分", 60),
            ("5分", 300),
            ("10分", 600),
        ],
        index=4,
        format_func=lambda x: x[0]
    )
    return int(interval_option[1]) if interval_option else 0

def styled_message(text, color, emoji=""):
    return f"<p style='color:{color}; font-weight:bold;'>{emoji} {text}</p>"

def main():
    st.set_page_config(page_title="ログ整形 ᔦ◎◎ᔨ", layout="centered", initial_sidebar_state="expanded")
    st.title("ログ整形 ᔦꙬᔨ")
    interval_seconds = select_interval()
    uploaded_files = st.file_uploader("CSVファイルをアップロードしてください", type=["csv"], accept_multiple_files=True)
    start_clicked = st.button("▶ スタート")

    if start_clicked:
        if not uploaded_files:
            st.warning("CSVファイルをアップロードしてください")
            st.stop()

        # 表示順を入れ替えて文字がプログレスバーの上に来るように
        progress_text = st.empty()
        progress = st.progress(0)

        # 処理時間計測用
        total_start = time.time()

        # 読み込み処理
        load_start = time.time()
        temp_dfs = []
        for idx, file in enumerate(uploaded_files, start=1):
            progress_val = int(idx / len(uploaded_files) * 30)
            progress_text.markdown(styled_message(f"[{idx}/{len(uploaded_files)}] {file.name} 読み込み中…", "blue", "🔵"), unsafe_allow_html=True)
            progress.progress(progress_val)
            df = load_csv(file)
            if df.empty:
                st.error(f"{file.name} に『日付』『時刻』列が見つかりません")
            else:
                temp_dfs.append(df)
        load_time = time.time() - load_start

        if not temp_dfs:
            st.stop()

        overall_base_time = min(df["datetime"].min() for df in temp_dfs)

        # 間引き処理
        filter_start = time.time()
        progress_text.markdown(styled_message("間引き中…", "orange", "🟠"), unsafe_allow_html=True)
        progress.progress(50)
        reduced_dfs = [filter_by_interval(df, interval_seconds, base_time=overall_base_time) for df in temp_dfs]
        filter_time = time.time() - filter_start

        # 結合処理
        merge_start = time.time()
        progress_text.markdown(styled_message("結合中…", "purple", "🟣"), unsafe_allow_html=True)
        progress.progress(80)
        merged_df = merge_and_sort(reduced_dfs)
        merge_time = time.time() - merge_start

        # 完了
        progress.progress(100)
        progress_text.markdown(styled_message("完了！", "green", "✅"), unsafe_allow_html=True)
        total_time = time.time() - total_start

        st.info(f"結合後の件数: {len(merged_df)} 件")
        if merged_df.empty:
            st.warning("抽出結果がありません")
            st.stop()

        # 処理時間の表示
        st.write(f"処理時間 (秒)：読み込み {load_time:.2f} | 間引き {filter_time:.2f} | 結合 {merge_time:.2f} | 合計 {total_time:.2f}")

        csv_bytes = generate_csv(merged_df)

        st.success(f"完了！ 合計 {len(merged_df)} 件を抽出しました")
        st.download_button(
            "ダウンロードはこちら🦀",
            csv_bytes,
            file_name="filtered_interval_data.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
