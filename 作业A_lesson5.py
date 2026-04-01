"""
英雄胜率分析脚本 - 连接 MySQL 数据库，查询并导出英雄战绩报表
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os

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
    conn = create_db_connection()
    
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
    df = pd.read_sql(sql_query, conn)
    
    # 转换胜率为百分比格式（保留一位小数）
    df['win_rate_percentage'] = df['win_rate'].apply(lambda x: f"{x*100:.1f}%")
    
    # 创建导出用的 DataFrame
    df_export = df[['hero_id', 'hero_name', 'total_games', 'win_games', 'win_rate_percentage']].copy()
    df_export.columns = ['英雄ID', '英雄名称', '总场次', '胜场数', '胜率']
    
    # 导出为 Excel
    df_export.to_excel(OUTPUT_FILE, index=False, sheet_name='英雄胜率')
    print(f"✓ 报表已导出到: {OUTPUT_FILE}")
    
    # 保存分析结果到数据库的 analysis_log 表
    save_to_analysis_log(df, conn)
    
    # 打印终端摘要
    print_summary(df)
    
    return df


def save_to_analysis_log(df, engine):
    """
    将分析结果保存到 analysis_log 表
    
    Args:
        df (pd.DataFrame): 包含英雄统计信息的 DataFrame
        engine: SQLAlchemy 数据库引擎
    """
    with engine.connect() as conn:
        for idx, row in df.iterrows():
            sql_insert = text("""
            INSERT INTO analysis_log 
            (hero_id, hero_name, total_games, win_games, win_rate, analyst, run_time)
            VALUES (:hero_id, :hero_name, :total_games, :win_games, :win_rate, :analyst, :run_time)
            """)
            
            run_time = datetime.now()
            
            values = {
                'hero_id': int(row['hero_id']),
                'hero_name': row['hero_name'],
                'total_games': int(row['total_games']),
                'win_games': int(row['win_games']),
                'win_rate': float(row['win_rate']),
                'analyst': ANALYST_NAME,
                'run_time': run_time
            }
            
            conn.execute(sql_insert, values)
        
        conn.commit()
    
    print(f"✓ 分析结果已保存到数据库 ({len(df)} 条记录)")


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
    print("="*50 + "\n")


if __name__ == '__main__':
    # 执行分析
    df_result = analyze_hero_winrate()
    
    print("任务完成！")
