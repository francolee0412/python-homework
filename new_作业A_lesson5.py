"""
英雄胜率分析脚本（重构版）
功能：连接 MySQL 数据库，查询英雄战绩，计算胜率，导出报表，并按计划定时执行
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from config import DB_CONFIG, ANALYST_NAME  # 敏感信息从 config.py 读取

# ==================== 配置区 ====================
MIN_GAMES = 30                           # 最低场次筛选阈值
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), 'hero_winrate.xlsx')  # 导出文件路径
LOG_FILE = os.path.join(os.path.dirname(__file__), 'task.log')              # 日志文件路径
SCHEDULE_INTERVAL_MINUTES = 2           # 定时执行间隔（分钟）


# ==================== 日志配置 ====================
def setup_logging():
    """配置日志：同时输出到屏幕和 task.log 文件"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 输出到屏幕
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 输出到文件
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setFormatter(formatter)

    # 避免重复添加 handler
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)


# ==================== 数据库连接 ====================
def create_db_connection():
    """
    创建并返回 SQLAlchemy 数据库引擎。
    连接失败时抛出异常并记录错误日志。
    """
    try:
        conn_str = (
            f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        engine = create_engine(conn_str)
        logging.info("数据库连接成功")
        return engine
    except Exception as err:
        logging.error(f"数据库连接失败: {err}")
        raise


# ==================== 数据查询与计算 ====================
def query_hero_winrate(engine):
    """
    从数据库查询英雄战绩数据，计算总场次、胜场数和胜率。
    筛选总场次 >= MIN_GAMES 的英雄，按胜率从高到低排序。
    返回包含统计结果的 DataFrame。
    """
    sql_query = f"""
    SELECT
        h.hero_id,
        h.hero_name,
        COUNT(br.record_id)                              AS total_games,
        SUM(br.is_win)                                   AS win_games,
        ROUND(SUM(br.is_win) / COUNT(br.record_id), 4)  AS win_rate
    FROM hero h
    LEFT JOIN battle_record br ON h.hero_id = br.hero_id
    WHERE br.record_id IS NOT NULL
    GROUP BY h.hero_id, h.hero_name
    HAVING total_games >= {MIN_GAMES}
    ORDER BY win_rate DESC
    """
    df = pd.read_sql(sql_query, engine)
    # 新增百分比格式列，保留一位小数
    df['win_rate_percentage'] = df['win_rate'].apply(lambda x: f"{x * 100:.1f}%")
    logging.info(f"查询完成，共获取 {len(df)} 条符合条件的英雄数据")
    return df


# ==================== 导出 Excel ====================
def export_to_excel(df):
    """
    将英雄胜率数据导出为 Excel 文件（hero_winrate.xlsx）。
    列名转换为中文，不包含行索引。
    """
    df_export = df[['hero_id', 'hero_name', 'total_games', 'win_games', 'win_rate_percentage']].copy()
    df_export.columns = ['英雄ID', '英雄名称', '总场次', '胜场数', '胜率']
    df_export.to_excel(OUTPUT_FILE, index=False, sheet_name='英雄胜率')
    logging.info(f"报表已导出到: {OUTPUT_FILE}")


# ==================== 写入分析日志表 ====================
def save_to_analysis_log(df, engine):
    """
    将本次分析结果逐行写入数据库的 analysis_log 表，
    记录分析人姓名和执行时间。
    """
    run_time = datetime.now()
    with engine.connect() as conn:
        for _, row in df.iterrows():
            sql_insert = text("""
            INSERT INTO analysis_log
                (hero_id, hero_name, total_games, win_games, win_rate, analyst, run_time)
            VALUES
                (:hero_id, :hero_name, :total_games, :win_games, :win_rate, :analyst, :run_time)
            """)
            conn.execute(sql_insert, {
                'hero_id':     int(row['hero_id']),
                'hero_name':   row['hero_name'],
                'total_games': int(row['total_games']),
                'win_games':   int(row['win_games']),
                'win_rate':    float(row['win_rate']),
                'analyst':     ANALYST_NAME,
                'run_time':    run_time,
            })
        conn.commit()
    logging.info(f"分析结果已写入数据库 analysis_log 表（{len(df)} 条记录）")


# ==================== 终端摘要打印 ====================
def print_summary(df):
    """
    在终端（及日志）打印本次分析的统计摘要：
    总英雄数、平均胜率、胜率最高的英雄。
    """
    top_hero = df.iloc[0]
    logging.info("=" * 50)
    logging.info("英雄胜率分析摘要")
    logging.info("=" * 50)
    logging.info(f"符合条件的英雄总数 : {len(df)} 个")
    logging.info(f"平均胜率           : {df['win_rate'].mean() * 100:.1f}%")
    logging.info(f"胜率最高的英雄     : {top_hero['hero_name']} ({top_hero['win_rate'] * 100:.1f}%)")
    logging.info("=" * 50)


# ==================== 主流程 ====================
def main():
    """
    主流程函数：依次执行数据库连接、数据查询、导出报表、
    写入日志表、打印摘要。供直接调用和调度器调用。
    """
    logging.info("===== 任务开始执行 =====")
    try:
        engine = create_db_connection()
        df = query_hero_winrate(engine)
        export_to_excel(df)
        save_to_analysis_log(df, engine)
        print_summary(df)
        logging.info("===== 任务执行完成 =====")
    except Exception as e:
        logging.error(f"任务执行出错: {e}")


# ==================== 入口 ====================
if __name__ == '__main__':
    setup_logging()

    # 启动时立即执行一次，方便验证效果
    logging.info("脚本启动，立即执行一次主流程")
    main()

    # 配置 APScheduler：每 SCHEDULE_INTERVAL_MINUTES 分钟执行一次
    scheduler = BlockingScheduler(timezone='Asia/Shanghai')
    scheduler.add_job(
        func=main,
        trigger='interval',
        minutes=SCHEDULE_INTERVAL_MINUTES,
        id='hero_winrate_job',
        name='英雄胜率定时分析'
    )

    logging.info(f"调度器已启动，每 {SCHEDULE_INTERVAL_MINUTES} 分钟执行一次 main()")
    logging.info("按 Ctrl+C 停止调度器")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("调度器已停止")
