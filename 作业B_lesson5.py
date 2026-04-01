"""
英雄胜率分析脚本 - 连接 MySQL 数据库，查询并导出英雄战绩报表
"""
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import sys

# 设置控制台编码为 UTF-8
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# ==================== 配置区 ====================
DB_CONFIG = {
    'host': '192.168.40.83',
    'port': 3306,
    'user': 'student',
    'password': 'mlbb2026',
    'database': 'homework_db'
}

ANALYST_NAME = '李卿'  # 修改为自己的名字
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), 'hero_winrate.xlsx')


# ==================== 连接函数 ====================
def create_db_connection():
    """
    创建数据库连接（SQLAlchemy 引擎）
    
    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy 数据库引擎
    """
    try:
        connection_string = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(connection_string)
        print("✓ 数据库连接成功")
        return engine
    except Exception as err:
        print(f"✗ 数据库连接失败: {err}")
        raise


# ==================== 主要逻辑 ====================
def analyze_hero_winrate():
    """
    主函数：查询数据、计算胜率、导出报表
    """
    
    # 连接数据库
    engine = create_db_connection()
    
    # SQL 查询：合并 hero 和 battle_record，计算每个英雄的统计数据
    sql_query = """
    SELECT 
        h.hero_id,
        h.hero_name,
        COUNT(br.record_id) AS total_games,
        SUM(br.is_win) AS win_games,
        ROUND(SUM(br.is_win) / COUNT(br.record_id), 4) AS win_rate
    FROM hero h
    LEFT JOIN battle_record br ON h.hero_id = br.hero_id
    WHERE br.record_id IS NOT NULL
    GROUP BY h.hero_id, h.hero_name
    HAVING total_games >= 30
    ORDER BY win_rate DESC
    """
    
    # 使用 pandas 读取查询结果
    df = pd.read_sql(sql_query, engine)
    
    # 添加 analyst 和 run_time 列
    df['analyst'] = ANALYST_NAME
    df['run_time'] = datetime.now()
    
    # 转换胜率为百分比格式（保留一位小数）用于 Excel 导出
    df['win_rate_percentage'] = df['win_rate'].apply(lambda x: f"{x*100:.1f}%")
    
    # 创建导出用的 DataFrame
    df_export = df[['hero_id', 'hero_name', 'total_games', 'win_games', 'win_rate_percentage']].copy()
    df_export.columns = ['英雄ID', '英雄名称', '总场次', '胜场数', '胜率']
    
    # 导出为 Excel（如果文件已存在则删除）
    if os.path.exists(OUTPUT_FILE):
        try:
            os.remove(OUTPUT_FILE)
        except Exception as e:
            print(f"⚠ 删除旧文件失败: {e}")
    
    df_export.to_excel(OUTPUT_FILE, index=False, sheet_name='英雄胜率')
    print(f"✓ 报表已导出到: {OUTPUT_FILE}")
    
    # 保存分析结果到数据库的 analysis_log 表
    save_to_analysis_log(df, engine)
    
    # 打印终端摘要
    print_summary(df)
    
    # 查询并打印所有分析结果
    print_all_analysis_logs(engine)
    
    # 打印思考题答案
    print_thinking_questions()
    
    return df


def save_to_analysis_log(df, engine):
    """
    将分析结果保存到 analysis_log 表（使用 to_sql 追加写入）
    
    Args:
        df (pd.DataFrame): 包含英雄统计信息的 DataFrame
        engine: SQLAlchemy 数据库引擎
    """
    # 只保留需要写入的列
    df_to_write = df[['hero_id', 'hero_name', 'total_games', 'win_games', 'win_rate', 'analyst', 'run_time']].copy()
    
    try:
        # 使用 to_sql 追加写入数据库
        df_to_write.to_sql('analysis_log', con=engine, if_exists='append', index=False)
        print(f"✓ 分析结果已保存到数据库 ({len(df_to_write)} 条记录)")
    except Exception as err:
        print(f"✗ 写入数据库失败: {err}")


def print_summary(df):
    """
    打印终端统计摘要
    
    Args:
        df (pd.DataFrame): 包含英雄统计信息的 DataFrame
    """
    print("\n" + "="*50)
    print("📊 英雄胜率分析摘要")
    print("="*50)
    print(f"符合条件的英雄总数: {len(df)} 个")
    print(f"平均胜率: {df['win_rate'].mean()*100:.1f}%")
    print(f"胜率最高的英雄: {df.iloc[0]['hero_name']} ({df.iloc[0]['win_rate']*100:.1f}%)")
    print(f"分析人: {ANALYST_NAME}")
    print(f"运行时间: {df.iloc[0]['run_time']}")
    print("="*50 + "\n")


def print_all_analysis_logs(engine):
    """
    查询并打印 analysis_log 表中所有人的记录
    
    Args:
        engine: SQLAlchemy 数据库引擎
    """
    sql_query = """
    SELECT 
        log_id,
        hero_id,
        hero_name,
        total_games,
        win_games,
        ROUND(win_rate * 100, 1) AS win_rate_percent,
        analyst,
        run_time
    FROM analysis_log
    ORDER BY run_time DESC
    """
    
    df_logs = pd.read_sql(sql_query, engine)
    
    print("\n" + "="*120)
    print("📋 analysis_log 表中所有记录")
    print("="*120)
    print(df_logs.to_string(index=False))
    print("="*120 + "\n")


def print_thinking_questions():
    """
    打印思考题答案
    """
    print("\n" + "="*120)
    print("💭 思考题答案")
    print("="*120)
    
    print("\n【问题 1】如果班里 10 个同学都运行了作业脚本，且脚本内容符合作业要求，analysis_log 表里会有多少行？")
    print("-" * 120)
    print("【答案】analysis_log 表里会有 200 行")
    print("\n原因分析：")
    print("  • 英雄数据：根据查询条件（总场次 >= 30），数据库中符合条件的英雄有 20 个")
    print("  • 同学人数：10 个同学")
    print("  • 总行数：20 个英雄 × 10 个同学 = 200 行")
    print("  • 每个同学都会插入 20 条记录（每个英雄一条），所以总共是 20 × 10 = 200 行")
    
    print("\n【问题 2】如果你运行了两次，如何用 SQL 只查出你自己最新一次的结果？")
    print("-" * 120)
    print("【SQL 答案 - 方法 1：使用 MAX(run_time) 子查询】")
    print()
    
    sql_query_1 = f"""
    SELECT 
        a.log_id,
        a.hero_id,
        a.hero_name,
        a.total_games,
        a.win_games,
        ROUND(a.win_rate * 100, 1) AS win_rate_percent,
        a.analyst,
        a.run_time
    FROM analysis_log a
    WHERE a.analyst = '{ANALYST_NAME}'
      AND a.run_time = (
            SELECT MAX(run_time) 
            FROM analysis_log 
            WHERE analyst = '{ANALYST_NAME}'
      )
    ORDER BY a.hero_id;
    """
    
    print(sql_query_1)
    print("【解释】")
    print(f"  • 使用子查询 MAX(run_time) 找出 '{ANALYST_NAME}' 最新一次运行的时间")
    print(f"  • 再用 WHERE 条件筛选出该时间对应的所有记录")
    print(f"  • 确保只返回最新一次运行的结果（20 条记录）")
    
    print("\n【SQL 答案 - 方法 2：使用 ROW_NUMBER() 窗口函数（推荐）】")
    print()
    
    sql_query_2 = f"""
    SELECT 
        log_id,
        hero_id,
        hero_name,
        total_games,
        win_games,
        ROUND(win_rate * 100, 1) AS win_rate_percent,
        analyst,
        run_time
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY analyst ORDER BY run_time DESC) AS rn
        FROM analysis_log
        WHERE analyst = '{ANALYST_NAME}'
    ) AS ranked_logs
    WHERE rn = 1
    ORDER BY hero_id;
    """
    
    print(sql_query_2)
    print("【窗口函数解释】")
    print("  • ROW_NUMBER() 为每个 analyst 的每条记录编号")
    print("  • PARTITION BY analyst：按分析人分组")
    print("  • ORDER BY run_time DESC：按时间降序排列，最新的记录编号为 1")
    print("  • WHERE rn = 1：只保留最新一次的记录")
    
    print("\n" + "="*120 + "\n")


if __name__ == '__main__':
    # 执行分析
    df_result = analyze_hero_winrate()
    
    print("任务完成！")
