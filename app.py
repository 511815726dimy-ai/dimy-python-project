# -*- coding: utf-8 -*-
"""
完整的6表协作系统 - NiceGUI 应用
法律+人力部门协作的财务合规系统
"""

from nicegui import ui, app
from starlette.requests import Request
from starlette.responses import JSONResponse
import sqlite3
import pandas as pd
from datetime import datetime
import os
import tempfile
import webbrowser
import threading
from pathlib import Path
import sys


sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from calculation_engine import CalculationEngine
from column_maps import COLUMN_MAP
DB_NAME = 'data.db'


def get_conn():
    return sqlite3.connect(DB_NAME)


# ==================== 初始化数据库 ====================

def init_database():
    """初始化数据库，创建所有表"""
    conn = get_conn()
    cursor = conn.cursor()

    # 读取 SQL 脚本
    sql_script = """
    -- 1. 人力工资表
    CREATE TABLE IF NOT EXISTS hr_payroll (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sequence_no TEXT,
        date TEXT NOT NULL,
        id_number TEXT,
        employee_id TEXT NOT NULL,
        employee_name TEXT,
        organization TEXT,
        department TEXT,
        front_middle_back TEXT,
        position_title TEXT,
        entry_date TEXT,
        remark TEXT,
        organization_level TEXT,
        organization_type TEXT,
        salary_category TEXT,
        salary_grade TEXT,
        salary_level TEXT,
        position_salary REAL,
        dept_monthly_perf REAL,
        line_perf REAL,
        dept_quarterly_perf REAL,
        line_special_award REAL,
        settlement_perf REAL,
        hard_target REAL,
        performance_adjustment REAL,
        total_perf REAL,
        deferred_ratio REAL,
        deferred_salary REAL,
        post_deferred_perf REAL,
        position_adjustment REAL,
        allowance REAL,
        study_award REAL,
        overtime REAL,
        award_punishment REAL,
        temp_allowance REAL,
        release_deferred REAL,
        previous_year_release REAL,
        attendance_deduct REAL,
        taxable_total REAL,
        personal_pension REAL,
        personal_medical REAL,
        personal_unemployment REAL,
        personal_pension_fund REAL,
        personal_pension_supp REAL,
        personal_medical_supp REAL,
        personal_unemployment_supp REAL,
        personal_pension_fund_supp REAL,
        personal_annuity REAL,
        income_tax REAL,
        tax_supp REAL,
        accountability REAL,
        union_fee REAL,
        actual_payment REAL,
        personnel_category TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(date, employee_id)
    );

    CREATE INDEX IF NOT EXISTS idx_hr_payroll_date_emp ON hr_payroll(date, employee_id);
    CREATE INDEX IF NOT EXISTS idx_hr_payroll_emp_year ON hr_payroll(employee_id, substr(date, 1, 4));

    -- 2. 人力花名册
    CREATE TABLE IF NOT EXISTS hr_roster (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sequence_no TEXT,
        employee_id TEXT NOT NULL UNIQUE,
        employee_name TEXT,
        organization TEXT,
        department TEXT,
        main_position TEXT,
        position_title TEXT,
        political_status TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_hr_roster_emp ON hr_roster(employee_id);

    -- 3. 人力风险金
    CREATE TABLE IF NOT EXISTS hr_risk_fund (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sequence_no TEXT,
        employee_id TEXT NOT NULL,
        employee_name TEXT,
        organization TEXT,
        working_status TEXT,
        year INTEGER NOT NULL,
        month_01 REAL DEFAULT 0,
        month_02 REAL DEFAULT 0,
        month_03 REAL DEFAULT 0,
        month_04 REAL DEFAULT 0,
        month_05 REAL DEFAULT 0,
        month_06 REAL DEFAULT 0,
        month_07 REAL DEFAULT 0,
        month_08 REAL DEFAULT 0,
        month_09 REAL DEFAULT 0,
        month_10 REAL DEFAULT 0,
        month_11 REAL DEFAULT 0,
        month_12 REAL DEFAULT 0,
        annual_deferred REAL,
        cumulative_deferred REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(employee_id, year)
    );

    CREATE INDEX IF NOT EXISTS idx_hr_risk_fund_emp_year ON hr_risk_fund(employee_id, year);

    -- 4. 人力风险金延伸
    CREATE TABLE IF NOT EXISTS hr_risk_fund_extended (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sequence_no TEXT,
        date TEXT NOT NULL,
        employee_id TEXT NOT NULL,
        employee_name TEXT,
        organization TEXT,
        working_status TEXT,
        n4_committee_defer REAL DEFAULT 0,
        n4_credit_deduct REAL DEFAULT 0,
        n4_bifurcated_defer REAL DEFAULT 0,
        n4_other REAL DEFAULT 0,
        n3_committee_defer REAL DEFAULT 0,
        n3_credit_deduct REAL DEFAULT 0,
        n3_bifurcated_defer REAL DEFAULT 0,
        n3_other REAL DEFAULT 0,
        n2_committee_defer REAL DEFAULT 0,
        n2_credit_deduct REAL DEFAULT 0,
        n2_bifurcated_defer REAL DEFAULT 0,
        n2_other REAL DEFAULT 0,
        remark TEXT,
        n4_balance REAL,
        n3_balance REAL,
        n2_balance REAL,
        balance_total REAL,
        n4_repay_pending REAL,
        n4_accountability_deduct REAL,
        n4_actual_repay REAL,
        n3_repay_pending REAL,
        n3_accountability_deduct REAL,
        n3_actual_repay REAL,
        n2_repay_pending REAL,
        n2_accountability_deduct REAL,
        n2_actual_repay REAL,
        annual_repay_total REAL,
        accountability_actual_tax_pre REAL,
        accountability_actual_tax_post REAL,
        accountability_provide_tax_pre REAL,
        accountability_provide_tax_post REAL,
        doc_number TEXT,
        n4_after_exec_balance REAL,
        n3_after_exec_balance REAL,
        n2_after_exec_balance REAL,
        final_balance REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(date, employee_id)
    );

    CREATE INDEX IF NOT EXISTS idx_hr_risk_ext_date_emp ON hr_risk_fund_extended(date, employee_id);

    -- 5. 法律经济处理明细表
    CREATE TABLE IF NOT EXISTS legal_economic_detail (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sequence_no TEXT,
        version INTEGER DEFAULT 1,
        date TEXT NOT NULL,
        employee_id TEXT NOT NULL,
        doc_number TEXT NOT NULL,
        employee_name TEXT,
        organization TEXT,
        file_name TEXT,
        issue_date TEXT,
        tax_pre REAL,
        tax_post REAL,
        discipline TEXT,
        salary_year INTEGER,
        coefficient REAL,
        remark TEXT,
        accounting_amount REAL,
        status TEXT DEFAULT '待下载',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(version, date, employee_id, doc_number)
    );

    CREATE INDEX IF NOT EXISTS idx_legal_econ_date_emp_doc ON legal_economic_detail(date, employee_id, doc_number);

    -- 6. 法律问责办台账
    
    CREATE TABLE IF NOT EXISTS legal_accountability (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sequence_no TEXT,
        branch TEXT,
        employee_id TEXT NOT NULL,
        employee_name TEXT,
        original_organization TEXT,
        original_position_category TEXT,
        original_position_title TEXT,
        current_organization TEXT,
        current_position_category TEXT,
        current_position_title TEXT,
        employee_status TEXT,
        violation_source TEXT,
        violation_attribution TEXT,
        violation_description TEXT,
        violation_discovery_date TEXT,
        accountability_project TEXT,
        doc_number TEXT NOT NULL,
        accountability_authority TEXT,
        decision_body TEXT,
        issue_date TEXT,
        handling_basis TEXT,
        discipline_type TEXT,
        tax_pre REAL,
        tax_post REAL,
        total_economic REAL,
        main_pay_tax_pre REAL,
        main_pay_tax_post REAL,
        exec_date_legal TEXT,
        exec_date_hr TEXT,
        criticism TEXT,
        remark TEXT,
        conference TEXT,
        political_status TEXT,
        performance_adjustment REAL,
        accountability_tax_post REAL,
        risk_deduction REAL,
        tax_pre_exec REAL,
        tax_pre_pending REAL,
        tax_post_exec REAL,
        tax_post_pending REAL,
        exec_total REAL,
        unexec_total REAL,
        cumulative_performance REAL,
        cumulative_risk REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(employee_id, doc_number, exec_date_hr)
    );

    CREATE INDEX IF NOT EXISTS idx_legal_acct_emp_date ON legal_accountability(employee_id, exec_date_hr);
    """

    for statement in sql_script.split(';'):
        if statement.strip():
            cursor.execute(statement)

    conn.commit()
    conn.close()
    print("✅ 数据库初始化完成")


init_database()
engine = CalculationEngine(DB_NAME)


# ==================== API 路由 - 导入处理 ====================

async def upload_hr_payroll(request: Request):
    """人力工资表导入"""
    try:
        form = await request.form()
        file = form.get('file')

        if not file:
            return JSONResponse({'success': False, 'message': '没有选择文件'})

        content = await file.read()
        if not content:
            return JSONResponse({'success': False, 'message': '文件为空'})

        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, f'hr_payroll_{datetime.now().timestamp()}.xlsx')

        with open(temp_path, 'wb') as f:
            f.write(content)

        try:
            df = pd.read_excel(temp_path, sheet_name=0, dtype={'date': str, 'employee_id': str})
            print(f"📊 工资表导入 - 读取列名: {list(df.columns)}")
        except Exception as e:
            os.remove(temp_path)
            return JSONResponse({'success': False, 'message': f'读取文件失败: {str(e)}'})

        if df.empty:
            os.remove(temp_path)
            return JSONResponse({'success': False, 'message': '文件中没有数据'})

        inserted = 0
        conn = get_conn()
        cursor = conn.cursor()

        # 列名映射
        column_mapping = {
            '序号': 'sequence_no',
            '日期': 'date',
            '身份证号': 'id_number',
            '员工号': 'employee_id',
            '姓名': 'employee_name',
            '机构': 'organization',
            '部门': 'department',
            '前中后台': 'front_middle_back',
            '岗位职务': 'position_title',
            '入行时间': 'entry_date',
            '备注': 'remark',
            '机构层级': 'organization_level',
            '机构类型': 'organization_type',
            '薪酬类别': 'salary_category',
            '工资等级': 'salary_grade',
            '工资档次': 'salary_level',
            '岗位工资': 'position_salary',
            '管理部门月绩效工资': 'dept_monthly_perf',
            '条线积分绩效': 'line_perf',
            '管理部门季度绩效': 'dept_quarterly_perf',
            '条线专项奖惩': 'line_special_award',
            '结算绩效工资': 'settlement_perf',
            '硬性指标挂钩': 'hard_target',
            '绩效工资调整项': 'performance_adjustment',
            '合计绩效工资': 'total_perf',
            '计提延期比例': 'deferred_ratio',
            '计提延期薪酬': 'deferred_salary',
            '计提后发放绩效工资': 'post_deferred_perf',
            '岗位工资调整额': 'position_adjustment',
            '津贴/补贴项': 'allowance',
            '自学成才奖励': 'study_award',
            '加班工资': 'overtime',
            '奖惩事项': 'award_punishment',
            '防暑降温费/取暖补贴': 'temp_allowance',
            '发放延期薪酬': 'release_deferred',
            '以前年度发放': 'previous_year_release',
            '考勤扣款合计': 'attendance_deduct',
            '应发合计': 'taxable_total',
            '个人养老': 'personal_pension',
            '个人医疗': 'personal_medical',
            '个人失业': 'personal_unemployment',
            '个人公积金': 'personal_pension_fund',
            '补扣个人养老': 'personal_pension_supp',
            '补扣个人医疗': 'personal_medical_supp',
            '补扣个人失业': 'personal_unemployment_supp',
            '补扣个人公积金': 'personal_pension_fund_supp',
            '个人企业年金': 'personal_annuity',
            '个人所得税': 'income_tax',
            '补扣税': 'tax_supp',
            '问责': 'accountability',
            '工会会员费': 'union_fee',
            '实发合计': 'actual_payment',
            '人员类别': 'personnel_category',
        }

        for idx, row in df.iterrows():
            try:
                date = str(row.get('日期', '')).strip() if pd.notna(row.get('日期')) else ''
                employee_id = str(row.get('员工号', '')).strip() if pd.notna(row.get('员工号')) else ''

                if not date or not employee_id:
                    continue

                # 构建插入数据
                insert_data = {'date': date, 'employee_id': employee_id}

                for cn, en in column_mapping.items():
                    if cn in df.columns:
                        val = row.get(cn)
                        if pd.isna(val):
                            insert_data[en] = None
                        elif en in ['position_salary', 'dept_monthly_perf', 'line_perf',
                                    'dept_quarterly_perf', 'line_special_award', 'settlement_perf',
                                    'hard_target', 'performance_adjustment', 'total_perf',
                                    'deferred_ratio', 'deferred_salary', 'post_deferred_perf',
                                    'position_adjustment', 'allowance', 'study_award', 'overtime',
                                    'award_punishment', 'temp_allowance', 'release_deferred',
                                    'previous_year_release', 'attendance_deduct', 'taxable_total',
                                    'personal_pension', 'personal_medical', 'personal_unemployment',
                                    'personal_pension_fund', 'personal_pension_supp',
                                    'personal_medical_supp', 'personal_unemployment_supp',
                                    'personal_pension_fund_supp', 'personal_annuity', 'income_tax',
                                    'tax_supp', 'accountability', 'union_fee', 'actual_payment']:
                            try:
                                insert_data[en] = float(val)
                            except:
                                insert_data[en] = 0
                        else:
                            insert_data[en] = str(val).strip()

                # 插入数据库
                columns = ', '.join(insert_data.keys())
                placeholders = ', '.join(['?' for _ in insert_data])
                sql = f"INSERT OR REPLACE INTO hr_payroll ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, list(insert_data.values()))

                inserted += 1

            except Exception as e:
                print(f"❌ 行 {idx} 导入失败: {e}")
                continue

        conn.commit()
        conn.close()
        os.remove(temp_path)

        # 导入完成后自动计算风险金
        engine.calculate_hr_risk_fund()
        engine.calculate_legal_economic_detail_v1()

        return JSONResponse({
            'success': True,
            'message': f'✅ 工资表导入成功！插入 {inserted} 条记录',
            'count': inserted
        })

    except Exception as e:
        print(f"工资表导入错误: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse({'success': False, 'message': f'服务器错误: {str(e)}'})


async def upload_hr_roster(request: Request):
    """人力花名册导入"""
    try:
        form = await request.form()
        file = form.get('file')

        if not file:
            return JSONResponse({'success': False, 'message': '没有选择文件'})

        content = await file.read()
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, f'hr_roster_{datetime.now().timestamp()}.xlsx')

        with open(temp_path, 'wb') as f:
            f.write(content)

        try:
            df = pd.read_excel(temp_path, sheet_name=0, dtype={'employee_id': str})
        except Exception as e:
            os.remove(temp_path)
            return JSONResponse({'success': False, 'message': f'读取文件失败: {str(e)}'})

        if df.empty:
            os.remove(temp_path)
            return JSONResponse({'success': False, 'message': '文件中没有数据'})

        inserted = 0
        conn = get_conn()
        cursor = conn.cursor()

        for idx, row in df.iterrows():
            try:
                employee_id = str(row.get('员工号', '')).strip()

                if not employee_id:
                    continue

                cursor.execute("""
                    INSERT OR REPLACE INTO hr_roster
                    (sequence_no, employee_id, employee_name, organization, 
                     department, main_position, position_title, political_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(row.get('序号', '')),
                    employee_id,
                    str(row.get('姓名', '')),
                    str(row.get('所属机构', '')),
                    str(row.get('部门/支行', '')),
                    str(row.get('主要岗位', '')),
                    str(row.get('职务', '')),
                    str(row.get('政治面貌', ''))
                ))

                inserted += 1

            except Exception as e:
                print(f"❌ 行 {idx} 导入失败: {e}")
                continue

        conn.commit()
        conn.close()
        os.remove(temp_path)

        return JSONResponse({
            'success': True,
            'message': f'✅ 花名册导入成功！插入 {inserted} 条记录',
            'count': inserted
        })

    except Exception as e:
        return JSONResponse({'success': False, 'message': f'服务器错误: {str(e)}'})


async def upload_legal_economic_detail(request: Request):
    """法律经济处理明细表导入"""
    try:
        form = await request.form()
        file = form.get('file')

        if not file:
            return JSONResponse({'success': False, 'message': '没有选择文件'})

        content = await file.read()
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, f'legal_econ_{datetime.now().timestamp()}.xlsx')

        with open(temp_path, 'wb') as f:
            f.write(content)

        try:
            df = pd.read_excel(temp_path, sheet_name=0, dtype={'date': str, 'employee_id': str, 'doc_number': str})
        except Exception as e:
            os.remove(temp_path)
            return JSONResponse({'success': False, 'message': f'读取文件失败: {str(e)}'})

        if df.empty:
            os.remove(temp_path)
            return JSONResponse({'success': False, 'message': '文件中没有数据'})

        inserted = 0
        conn = get_conn()
        cursor = conn.cursor()

        for idx, row in df.iterrows():
            try:
                date = str(row.get('日期', '')).strip()
                employee_id = str(row.get('员工号', '')).strip()
                doc_number = str(row.get('文号', '')).strip()

                if not all([date, employee_id, doc_number]):
                    continue

                try:
                    tax_pre = float(row.get('税前', 0)) if pd.notna(row.get('税前')) else 0
                    tax_post = float(row.get('税后', 0)) if pd.notna(row.get('税后')) else 0
                    coefficient = float(row.get('系数', 1)) if pd.notna(row.get('系数')) else 1
                    salary_year = int(row.get('工资表年度', date[:4])) if pd.notna(row.get('工资表年度')) else int(
                        date[:4])
                except:
                    continue

                version = 1 if '版本' not in row or pd.isna(row.get('版本')) else int(row.get('版本'))

                cursor.execute("""
                    INSERT OR REPLACE INTO legal_economic_detail
                    (version, sequence_no, date, employee_id, doc_number, employee_name,
                     organization, file_name, issue_date, tax_pre, tax_post, discipline,
                     salary_year, coefficient, remark, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    version,
                    str(row.get('序号', '')),
                    date,
                    employee_id,
                    doc_number,
                    str(row.get('被处理人', '')),
                    str(row.get('所在机构', '')),
                    str(row.get('处分文件名称', '')),
                    str(row.get('问责发文时间', '')),
                    tax_pre,
                    tax_post,
                    str(row.get('纪律处分', '')),
                    salary_year,
                    coefficient,
                    str(row.get('备注', '')),
                    '已重新上传' if version > 1 else '待下载'
                ))

                inserted += 1

            except Exception as e:
                print(f"❌ 行 {idx} 导入失败: {e}")
                continue

        conn.commit()
        conn.close()
        os.remove(temp_path)

        # 导入后自动计算
        engine.calculate_legal_economic_detail_v1()

        return JSONResponse({
            'success': True,
            'message': f'✅ 经济处理明细表导入成功！插入 {inserted} 条记录',
            'count': inserted
        })

    except Exception as e:
        return JSONResponse({'success': False, 'message': f'服务器错误: {str(e)}'})

async def upload_hr_risk_fund(request: Request):
    """人力风险金导入"""
    try:
        form = await request.form()
        file = form.get('file')

        if not file:
            return JSONResponse({'success': False, 'message': '没有选择文件'})

        content = await file.read()
        if not content:
            return JSONResponse({'success': False, 'message': '文件为空'})

        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, f'hr_risk_fund_{datetime.now().timestamp()}.xlsx')

        with open(temp_path, 'wb') as f:
            f.write(content)

        try:
            df = pd.read_excel(
                temp_path,
                sheet_name=0,
                dtype={'employee_id': str}
            )
            print(f"📊 风险金导入 - 读取列名: {list(df.columns)}")
        except Exception as e:
            os.remove(temp_path)
            return JSONResponse({'success': False, 'message': f'读取文件失败: {str(e)}'})

        if df.empty:
            os.remove(temp_path)
            return JSONResponse({'success': False, 'message': '文件中没有数据'})

        inserted = 0
        conn = get_conn()
        cursor = conn.cursor()

        for idx, row in df.iterrows():
            try:

                employee_id = str(row.get('员工号', '')).strip()
                year = row.get('年份（N）')

                if not employee_id or pd.isna(year):
                    continue

                try:
                    year = int(year)
                except:
                    continue

                def get_float(col):
                    val = row.get(col)
                    try:
                        return float(val) if pd.notna(val) else 0
                    except:
                        return 0

                cursor.execute("""
                    INSERT OR REPLACE INTO hr_risk_fund
                    (
                        sequence_no,
                        employee_id,
                        employee_name,
                        organization,
                        working_status,
                        year,
                        month_01,
                        month_02,
                        month_03,
                        month_04,
                        month_05,
                        month_06,
                        month_07,
                        month_08,
                        month_09,
                        month_10,
                        month_11,
                        month_12,
                        annual_deferred,
                        cumulative_deferred
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(row.get('序号', '')),
                    employee_id,
                    str(row.get('姓名', '')),
                    str(row.get('机构', '')),
                    str(row.get('在岗状态', '')),
                    year,

                    get_float('1月'),
                    get_float('2月'),
                    get_float('3月'),
                    get_float('4月'),
                    get_float('5月'),
                    get_float('6月'),
                    get_float('7月'),
                    get_float('8月'),
                    get_float('9月'),
                    get_float('10月'),
                    get_float('11月'),
                    get_float('12月'),

                    get_float('当年累计延期薪酬余额'),
                    get_float('累计延期薪酬余额')
                ))

                inserted += 1

            except Exception as e:
                print(f"❌ 行 {idx} 导入失败: {e}")
                import traceback
                traceback.print_exc()
                continue

        conn.commit()
        conn.close()
        os.remove(temp_path)

        # 导入后自动计算
        engine.calculate_hr_risk_fund()

        return JSONResponse({
            'success': True,
            'message': f'✅ 风险金导入成功！插入 {inserted} 条记录',
            'count': inserted
        })

    except Exception as e:
        print(f"风险金导入错误: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse({'success': False, 'message': f'服务器错误: {str(e)}'})

async def upload_hr_risk_fund_extended(request: Request):
    """人力风险金延伸导入"""
    try:
        form = await request.form()
        file = form.get('file')

        if not file:
            return JSONResponse({'success': False, 'message': '没有选择文件'})

        content = await file.read()
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, f'risk_ext_{datetime.now().timestamp()}.xlsx')

        with open(temp_path, 'wb') as f:
            f.write(content)

        try:
            df = pd.read_excel(temp_path, sheet_name=0, dtype={'date': str, 'employee_id': str})
        except Exception as e:
            os.remove(temp_path)
            return JSONResponse({'success': False, 'message': f'读取文件失败: {str(e)}'})

        if df.empty:
            os.remove(temp_path)
            return JSONResponse({'success': False, 'message': '文件中没有数据'})

        inserted = 0
        conn = get_conn()
        cursor = conn.cursor()

        for idx, row in df.iterrows():
            try:
                date = str(row.get('日期', '')).strip()
                employee_id = str(row.get('员工号', '')).strip()

                if not date or not employee_id:
                    continue

                # 提取所有数值字段（缓发和扣减）
                def get_float(col_name):
                    val = row.get(col_name)
                    try:
                        return float(val) if pd.notna(val) else 0
                    except:
                        return 0

                cursor.execute("""
                    INSERT OR REPLACE INTO hr_risk_fund_extended
                    (sequence_no, date, employee_id, employee_name, organization, working_status,
                     n4_committee_defer, n4_credit_deduct, n4_bifurcated_defer, n4_other,
                     n3_committee_defer, n3_credit_deduct, n3_bifurcated_defer, n3_other,
                     n2_committee_defer, n2_credit_deduct, n2_bifurcated_defer, n2_other,
                     remark)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(row.get('序号', '')),
                    date,
                    employee_id,
                    str(row.get('姓名', '')),
                    str(row.get('机构', '')),
                    str(row.get('在岗状态', '')),
                    get_float('N-4年纪委办缓发'),
                    get_float('N-4年授信扣减'),
                    get_float('N-4年二分缓发'),
                    get_float('N-4年其他'),
                    get_float('N-3年纪委办缓发'),
                    get_float('N-3年授信扣减'),
                    get_float('N-3年二分缓发'),
                    get_float('N-3年其他'),
                    get_float('N-2年纪委办缓发'),
                    get_float('N-2年授信扣减'),
                    get_float('N-2年二分缓发'),
                    get_float('N-2年其他'),
                    str(row.get('备注', ''))
                ))

                inserted += 1

            except Exception as e:
                print(f"❌ 行 {idx} 导入失败: {e}")
                import traceback
                traceback.print_exc()
                continue

        conn.commit()
        conn.close()
        os.remove(temp_path)

        # 导入后自动计算
        engine.calculate_hr_risk_fund_extended()

        return JSONResponse({
            'success': True,
            'message': f'✅ 风险金延伸导入成功！插入 {inserted} 条记录',
            'count': inserted
        })

    except Exception as e:
        print(f"风险金延伸导入错误: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse({'success': False, 'message': f'服务器错误: {str(e)}'})


async def upload_legal_accountability(request: Request):
    """法律问责办台账导入"""
    try:
        form = await request.form()
        file = form.get('file')

        if not file:
            return JSONResponse({'success': False, 'message': '没有选择文件'})

        content = await file.read()
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, f'legal_acct_{datetime.now().timestamp()}.xlsx')

        with open(temp_path, 'wb') as f:
            f.write(content)

        try:
            df = pd.read_excel(temp_path, sheet_name=0,
                               dtype={'employee_id': str, 'doc_number': str, 'exec_date_hr': str})
        except Exception as e:
            os.remove(temp_path)
            return JSONResponse({'success': False, 'message': f'读取文件失败: {str(e)}'})

        if df.empty:
            os.remove(temp_path)
            return JSONResponse({'success': False, 'message': '文件中没有数据'})

        inserted = 0
        conn = get_conn()
        cursor = conn.cursor()

        for idx, row in df.iterrows():
            try:
                employee_id = str(row.get('员工号', '')).strip()
                doc_number = str(row.get('文号', '')).strip()
                exec_date_hr = str(row.get('执行日期人力用', '')).strip()

                if not all([employee_id, doc_number, exec_date_hr]):
                    continue

                def get_float(col_name):
                    val = row.get(col_name)
                    try:
                        return float(val) if pd.notna(val) else 0
                    except:
                        return 0

                cursor.execute("""
                    INSERT OR REPLACE INTO legal_accountability
                    (sequence_no, branch, employee_id, employee_name,
                     original_organization, original_position_category, original_position_title,
                     current_organization, current_position_category, current_position_title,
                     employee_status, violation_source, violation_attribution, violation_description,
                     violation_discovery_date, accountability_project, doc_number,
                     accountability_authority, decision_body, issue_date, handling_basis,
                     discipline_type, tax_pre, tax_post, total_economic,
                     main_pay_tax_pre, main_pay_tax_post, exec_date_legal, exec_date_hr,
                     criticism, remark, conference)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(row.get('序号', '')),
                    str(row.get('分行', '')),
                    employee_id,
                    str(row.get('被处理人姓名', '')),
                    str(row.get('原所在机构（部门）', '')),
                    str(row.get('原岗位类别', '')),
                    str(row.get('原岗位职务', '')),
                    str(row.get('现所在机构（部门）', '')),
                    str(row.get('现岗位类别', '')),
                    str(row.get('现岗位职务', '')),
                    str(row.get('员工状态', '')),
                    str(row.get('违规事实来源', '')),
                    str(row.get('违规事实归属', '')),
                    str(row.get('违规事实概述', '')),
                    str(row.get('违规事实发现时间', '')),
                    str(row.get('问责项目名称', '')),
                    doc_number,
                    str(row.get('问责权限', '')),
                    str(row.get('问责决策机构', '')),
                    str(row.get('问责下达时间', '')),
                    str(row.get('处理依据', '')),
                    str(row.get('纪律处分类型', '')),
                    get_float('税前'),
                    get_float('税后'),
                    get_float('合计经济处理金额'),
                    get_float('主要缴纳金额税前'),
                    get_float('主要缴纳金额税后'),
                    str(row.get('执行日期法律用', '')),
                    exec_date_hr,
                    str(row.get('批评教育', '')),
                    str(row.get('备注说明', '')),
                    str(row.get('会议', ''))
                ))

                inserted += 1

            except Exception as e:
                print(f"❌ 行 {idx} 导入失败: {e}")
                import traceback
                traceback.print_exc()
                continue

        conn.commit()
        conn.close()
        os.remove(temp_path)

        # 导入后自动计算
        engine.calculate_legal_accountability()

        return JSONResponse({
            'success': True,
            'message': f'✅ 问责办台账导入成功！插入 {inserted} 条记录',
            'count': inserted
        })

    except Exception as e:
        print(f"问责办台账导入错误: {e}")
        return JSONResponse({'success': False, 'message': f'服务���错误: {str(e)}'})


# ==================== 注册 API 路由 ====================

@app.post('/upload_hr_payroll')
async def handle_upload_hr_payroll(request: Request):
    return await upload_hr_payroll(request)


@app.post('/upload_hr_roster')
async def handle_upload_hr_roster(request: Request):
    return await upload_hr_roster(request)


@app.post('/upload_legal_economic_detail')
async def handle_upload_legal_economic_detail(request: Request):
    return await upload_legal_economic_detail(request)


@app.post('/upload_hr_risk_fund_extended')
async def handle_upload_hr_risk_fund_extended(request: Request):
    return await upload_hr_risk_fund_extended(request)


@app.post('/upload_legal_accountability')
async def handle_upload_legal_accountability(request: Request):
    return await upload_legal_accountability(request)


# ==================== UI 页面 ====================

@ui.page('/')
def main_page():
    ui.label('📋 6表协作系统 - 法律+人力部门财务合规').classes('text-h3').style('color: #2196F3')
    ui.separator()

    with ui.tabs() as tabs:
        ui.tab('tab1', label='1️⃣ 基础导入')
        ui.tab('tab2', label='2️⃣ 自动计算')
        ui.tab('tab3', label='3️⃣ 法律部二次处理')
        ui.tab('tab4', label='4️⃣ 其他数据导入')
        ui.tab('tab5', label='5️⃣ 数据导出')
        ui.tab('tab6', label='❓ 帮助')

    with ui.tab_panels(tabs, value='tab1').classes('w-full'):




        # ========== Tab 1: 基础导入 ==========
        with ui.tab_panel('tab1'):
            ui.label('人力部导入基础数据').classes('text-h6')

            with ui.row().classes('gap-4'):
                # 工资表上传
                with ui.column().classes('w-1/2'):
                    ui.label('📊 人力工资表').classes('text-base font-bold')
                    ui.label('YYYYMM 格式日期').classes('text-xs text-gray-500')

                    upload_html_payroll = '''
                    <div style="padding: 20px; border: 2px dashed #FF9800; border-radius: 8px; background-color: #fff3e0;">
                        <input type="file" id="payrollFile" accept=".xlsx,.xls" style="display: none;">
                        <button id="payrollChooseBtn" style="width: 100%; padding: 12px; background-color: #FF9800; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; margin-bottom: 10px;">
                            📁 选择文件
                        </button>
                        <button id="payrollUploadBtn" style="width: 100%; padding: 12px; background-color: #FF9800; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold;">
                            📤 上传工资表
                        </button>
                        <div id="payrollFileName" style="margin-top: 10px; text-align: center; font-weight: bold; color: #FF9800;"></div>
                        <div id="payrollStatus" style="margin-top: 10px; text-align: center; color: #666;"></div>
                    </div>
                    '''
                    ui.html(upload_html_payroll)  # ✅ 正确

                # 花名册上传
                with ui.column().classes('w-1/2'):
                    ui.label('👥 人力花名册').classes('text-base font-bold')
                    ui.label('员工基本信息').classes('text-xs text-gray-500')

                    upload_html_roster = '''
                    <div style="padding: 20px; border: 2px dashed #4CAF50; border-radius: 8px; background-color: #e8f5e9;">
                        <input type="file" id="rosterFile" accept=".xlsx,.xls" style="display: none;">
                        <button id="rosterChooseBtn" style="width: 100%; padding: 12px; background-color: #4CAF50; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; margin-bottom: 10px;">
                            📁 选择文件
                        </button>
                        <button id="rosterUploadBtn" style="width: 100%; padding: 12px; background-color: #4CAF50; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold;">
                            📤 上传花名册
                        </button>
                        <div id="rosterFileName" style="margin-top: 10px; text-align: center; font-weight: bold; color: #4CAF50;"></div>
                        <div id="rosterStatus" style="margin-top: 10px; text-align: center; color: #666;"></div>
                    </div>
                    '''
                    ui.html(upload_html_roster)

            # JavaScript 处理
            ui.run_javascript('''
                // 工资表上传
                const payrollFile = document.getElementById('payrollFile');
                const payrollChooseBtn = document.getElementById('payrollChooseBtn');
                const payrollUploadBtn = document.getElementById('payrollUploadBtn');
                const payrollFileName = document.getElementById('payrollFileName');
                const payrollStatus = document.getElementById('payrollStatus');

                payrollChooseBtn.addEventListener('click', () => payrollFile.click());
                payrollFile.addEventListener('change', function() {
                    if (this.files[0]) {
                        payrollFileName.textContent = '✅ 已选择: ' + this.files[0].name;
                    }
                });

                payrollUploadBtn.addEventListener('click', async function() {
                    const file = payrollFile.files[0];
                    if (!file) {
                        payrollStatus.textContent = '❌ 请先选择文件';
                        payrollStatus.style.color = 'red';
                        return;
                    }

                    payrollStatus.textContent = '📥 上传中...';
                    payrollStatus.style.color = '#666';
                    payrollUploadBtn.disabled = true;

                    const formData = new FormData();
                    formData.append('file', file);

                    try {
                        const response = await fetch('/upload_hr_payroll', {
                            method: 'POST',
                            body: formData
                        });

                        const data = await response.json();
                        if (data.success) {
                            payrollStatus.textContent = '✅ ' + data.message;
                            payrollStatus.style.color = 'green';
                            payrollFileName.textContent = '';
                            payrollFile.value = '';
                            payrollUploadBtn.disabled = false;
                            setTimeout(() => location.reload(), 2000);
                        } else {
                            payrollStatus.textContent = '❌ ' + data.message;
                            payrollStatus.style.color = 'red';
                            payrollUploadBtn.disabled = false;
                        }
                    } catch (error) {
                        payrollStatus.textContent = '❌ 上传失败: ' + error.message;
                        payrollStatus.style.color = 'red';
                        payrollUploadBtn.disabled = false;
                    }
                });

                // 花名册上传
                const rosterFile = document.getElementById('rosterFile');
                const rosterChooseBtn = document.getElementById('rosterChooseBtn');
                const rosterUploadBtn = document.getElementById('rosterUploadBtn');
                const rosterFileName = document.getElementById('rosterFileName');
                const rosterStatus = document.getElementById('rosterStatus');

                rosterChooseBtn.addEventListener('click', () => rosterFile.click());
                rosterFile.addEventListener('change', function() {
                    if (this.files[0]) {
                        rosterFileName.textContent = '✅ 已选择: ' + this.files[0].name;
                    }
                });

                rosterUploadBtn.addEventListener('click', async function() {
                    const file = rosterFile.files[0];
                    if (!file) {
                        rosterStatus.textContent = '❌ 请先选择文件';
                        rosterStatus.style.color = 'red';
                        return;
                    }

                    rosterStatus.textContent = '📥 上传中...';
                    rosterStatus.style.color = '#666';
                    rosterUploadBtn.disabled = true;

                    const formData = new FormData();
                    formData.append('file', file);

                    try {
                        const response = await fetch('/upload_hr_roster', {
                            method: 'POST',
                            body: formData
                        });

                        const data = await response.json();
                        if (data.success) {
                            rosterStatus.textContent = '✅ ' + data.message;
                            rosterStatus.style.color = 'green';
                            rosterFileName.textContent = '';
                            rosterFile.value = '';
                            rosterUploadBtn.disabled = false;
                            setTimeout(() => location.reload(), 2000);
                        } else {
                            rosterStatus.textContent = '❌ ' + data.message;
                            rosterStatus.style.color = 'red';
                            rosterUploadBtn.disabled = false;
                        }
                    } catch (error) {
                        rosterStatus.textContent = '❌ 上传失败: ' + error.message;
                        rosterStatus.style.color = 'red';
                        rosterUploadBtn.disabled = false;
                    }
                });
            ''')

        # ========== Tab 2: 自动计算结果 ==========
        with ui.tab_panel('tab2'):
            ui.label('系统自动计算的表').classes('text-h6')

            with ui.tabs() as subtabs:
                # 人力风险金
                with ui.tab('人力风险金'):
                    conn = get_conn()
                    df = pd.read_sql("""
                        SELECT * FROM hr_risk_fund ORDER BY employee_id, year DESC LIMIT 20
                    """, conn)
                    conn.close()

                    if not df.empty:
                        ui.table.from_pandas(df).classes('w-full')

                        def download_risk_fund():
                            conn = get_conn()
                            df = pd.read_sql("""
                                SELECT * FROM hr_risk_fund ORDER BY employee_id, year
                            """, conn)
                            conn.close()
                            df.to_excel('人力风险金.xlsx', index=False)
                            ui.download('人力风险金.xlsx')

                        ui.button('📥 下载人力风险金', on_click=download_risk_fund).classes('bg-blue-500 text-white')
                    else:
                        ui.label('暂无数据 - 请先导入工资表').classes('text-gray-500')

                # 人力风险金延伸
                with ui.tab('人力风险金延伸'):
                    conn = get_conn()
                    df = pd.read_sql("""
                        SELECT * FROM hr_risk_fund_extended ORDER BY employee_id, date DESC LIMIT 20
                    """, conn)
                    conn.close()

                    if not df.empty:
                        ui.table.from_pandas(df).classes('w-full')

                        def download_risk_extended():
                            conn = get_conn()
                            df = pd.read_sql("""
                                SELECT * FROM hr_risk_fund_extended ORDER BY employee_id, date
                            """, conn)
                            conn.close()
                            df.to_excel('人力风险金延伸.xlsx', index=False)
                            ui.download('人力风险金延伸.xlsx')

                        ui.button('📥 下载人力风险金延伸', on_click=download_risk_extended).classes(
                            'bg-blue-500 text-white')
                    else:
                        ui.label('暂无数据 - 请先导入工资表和风险金延伸初始数据').classes('text-gray-500')

                # 法律经济处理明细
                with ui.tab('法律经济处理明细'):
                    conn = get_conn()
                    df = pd.read_sql("""
                        SELECT * FROM legal_economic_detail WHERE version = 1 ORDER BY date DESC LIMIT 20
                    """, conn)
                    conn.close()

                    if not df.empty:
                        ui.table.from_pandas(df).classes('w-full')

                        def download_econ():
                            conn = get_conn()
                            df = pd.read_sql("""
                                SELECT * FROM legal_economic_detail WHERE version = 1 ORDER BY date
                            """, conn)
                            conn.close()
                            df.to_excel('法律经济处理明细表.xlsx', index=False)
                            ui.download('法律经济处理明细表.xlsx')

                        ui.button('📥 下载经济处理明细表(v1)', on_click=download_econ).classes('bg-blue-500 text-white')
                    else:
                        ui.label('暂无数据 - 请先导入').classes('text-gray-500')

                # 法律问责办台账
                with ui.tab('法律问责办台账'):
                    conn = get_conn()
                    df = pd.read_sql("""
                        SELECT * FROM legal_accountability ORDER BY employee_id, exec_date_hr DESC LIMIT 20
                    """, conn)
                    conn.close()

                    if not df.empty:
                        ui.table.from_pandas(df).classes('w-full')

                        def download_acct():
                            conn = get_conn()
                            df = pd.read_sql("""
                                SELECT * FROM legal_accountability ORDER BY employee_id, exec_date_hr
                            """, conn)
                            conn.close()
                            df.to_excel('法律问责办台账.xlsx', index=False)
                            ui.download('法律问责办台账.xlsx')

                        ui.button('📥 下载问责办台账', on_click=download_acct).classes('bg-blue-500 text-white')
                    else:
                        ui.label('暂无数据 - 请先导入').classes('text-gray-500')

        # ========== Tab 3: 法律部二次处理 ==========
        with ui.tab_panel('tab3'):
            ui.label('⚠️ 法律部工作流程').classes('text-h6').style('color: #FF6B6B')

            ui.markdown('''
            **操作步骤：**

            1. 📥 **下载** 经济处理明细表(v1) - 包含自动计算的 **核算金额**
            2. ✏️ **修改** 表中的 **税前** 和 **税后** 两列
            3. 📤 **重新上传** 经济处理明细表(v2)
            4. ✅ 系统自动更新关联表
            ''')

            with ui.row().classes('gap-4'):
                def download_for_legal():
                    conn = get_conn()
                    df = pd.read_sql("""
                        SELECT * FROM legal_economic_detail WHERE version = 1
                    """, conn)
                    conn.close()

                    if df.empty:
                        ui.notify('暂无数据', color='warning')
                        return

                    df.to_excel('法律经济处理明细表_v1.xlsx', index=False)
                    ui.download('法律经济处理明细表_v1.xlsx')

                ui.button('📥 下载 v1 版本（修改用）', on_click=download_for_legal).classes('bg-FF6B6B text-white')

                # 重新上传
                upload_html_reupload = '''
                <div style="padding: 20px; border: 2px dashed #2196F3; border-radius: 8px; background-color: #e3f2fd;">
                    <input type="file" id="reuplaodFile" accept=".xlsx,.xls" style="display: none;">
                    <button id="reuplaodChooseBtn" style="width: 100%; padding: 12px; background-color: #FF9800; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; margin-bottom: 10px;">
                        📁 选择已修改的文件
                    </button>
                    <button id="reuplaodUploadBtn" style="width: 100%; padding: 12px; background-color: #2196F3; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold;">
                        📤 重新上传 v2
                    </button>
                    <div id="reuplaodFileName" style="margin-top: 10px; text-align: center; font-weight: bold; color: #2196F3;"></div>
                    <div id="reuplaodStatus" style="margin-top: 10px; text-align: center; color: #666;"></div>
                </div>
                '''
                ui.html(upload_html_reupload)

            ui.run_javascript('''
                const reuplaodFile = document.getElementById('reuplaodFile');
                const reuplaodChooseBtn = document.getElementById('reuplaodChooseBtn');
                const reuplaodUploadBtn = document.getElementById('reuplaodUploadBtn');
                const reuplaodFileName = document.getElementById('reuplaodFileName');
                const reuplaodStatus = document.getElementById('reuplaodStatus');

                reuplaodChooseBtn.addEventListener('click', () => reuplaodFile.click());
                reuplaodFile.addEventListener('change', function() {
                    if (this.files[0]) {
                        reuplaodFileName.textContent = '✅ 已选择: ' + this.files[0].name;
                    }
                });

                reuplaodUploadBtn.addEventListener('click', async function() {
                    const file = reuplaodFile.files[0];
                    if (!file) {
                        reuplaodStatus.textContent = '❌ 请先选择文件';
                        reuplaodStatus.style.color = 'red';
                        return;
                    }

                    reuplaodStatus.textContent = '📥 上传中...';
                    reuplaodStatus.style.color = '#666';
                    reuplaodUploadBtn.disabled = true;

                    const formData = new FormData();
                    formData.append('file', file);

                    try {
                        const response = await fetch('/upload_legal_economic_detail', {
                            method: 'POST',
                            body: formData
                        });

                        const data = await response.json();
                        if (data.success) {
                            reuplaodStatus.textContent = '✅ ' + data.message;
                            reuplaodStatus.style.color = 'green';
                            reuplaodFileName.textContent = '';
                            reuplaodFile.value = '';
                            reuplaodUploadBtn.disabled = false;
                            setTimeout(() => location.reload(), 2000);
                        } else {
                            reuplaodStatus.textContent = '❌ ' + data.message;
                            reuplaodStatus.style.color = 'red';
                            reuplaodUploadBtn.disabled = false;
                        }
                    } catch (error) {
                        reuplaodStatus.textContent = '❌ 上传失败: ' + error.message;
                        reuplaodStatus.style.color = 'red';
                        reuplaodUploadBtn.disabled = false;
                    }
                });
            ''')

        # ========== Tab 4: 其他导入 ==========
        with ui.tab_panel('tab4'):
            ui.label('风险金延伸 & 问责办台账导入').classes('text-h6')

            with ui.row().classes('gap-4'):
                # 法律经济处理明细
                with ui.column().classes('w-1/2'):
                    ui.label('📑 法律经济处理明细表').classes('text-base font-bold')

                    upload_html_legal = '''
                    <div style="padding: 20px; border: 2px dashed #3F51B5; border-radius: 8px; background-color: #e8eaf6;">
                        <input type="file" id="legalDetailFile" accept=".xlsx,.xls" style="display: none;">
                        <button id="legalDetailChooseBtn" style="width: 100%; padding: 12px; background-color: #FF9800; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; margin-bottom: 10px;">
                            📁 选择文件
                        </button>
                        <button id="legalDetailUploadBtn" style="width: 100%; padding: 12px; background-color: #3F51B5; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold;">
                            📤 上传法律明细
                        </button>
                        <div id="legalDetailFileName" style="margin-top: 10px; text-align: center; font-weight: bold; color: #3F51B5;"></div>
                        <div id="legalDetailStatus" style="margin-top: 10px; text-align: center; color: #666;"></div>
                    </div>
                    '''
                    ui.html(upload_html_legal)

                # 问责办台账
                with ui.column().classes('w-1/2'):
                    ui.label('📊 法律问责办台账').classes('text-base font-bold')

                    upload_html_acct = '''
                    <div style="padding: 20px; border: 2px dashed #00BCD4; border-radius: 8px; background-color: #e0f2f1;">
                        <input type="file" id="acctFile" accept=".xlsx,.xls" style="display: none;">
                        <button id="acctChooseBtn" style="width: 100%; padding: 12px; background-color: #FF9800; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; margin-bottom: 10px;">
                            📁 选择文件
                        </button>
                        <button id="acctUploadBtn" style="width: 100%; padding: 12px; background-color: #00BCD4; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold;">
                            📤 上传问责办台账
                        </button>
                        <div id="acctFileName" style="margin-top: 10px; text-align: center; font-weight: bold; color: #00BCD4;"></div>
                        <div id="acctStatus" style="margin-top: 10px; text-align: center; color: #666;"></div>
                    </div>
                    '''
                    ui.html(upload_html_acct)

            with ui.row().classes('gap-4 mt-4'):
                # 人力风险金
                with ui.column().classes('w-1/2'):
                    ui.label('💰 人力风险金').classes('text-base font-bold')

                    upload_html_risk = '''
                    <div style="padding: 20px; border: 2px dashed #4CAF50; border-radius: 8px; background-color: #e8f5e9;">
                        <input type="file" id="riskFundFile" accept=".xlsx,.xls" style="display: none;">
                        <button id="riskFundChooseBtn" style="width: 100%; padding: 12px; background-color: #FF9800; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; margin-bottom: 10px;">
                            📁 选择文件
                        </button>
                        <button id="riskFundUploadBtn" style="width: 100%; padding: 12px; background-color: #4CAF50; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold;">
                            📤 上传人力风险金
                        </button>
                        <div id="riskFundFileName" style="margin-top: 10px; text-align: center; font-weight: bold; color: #4CAF50;"></div>
                        <div id="riskFundStatus" style="margin-top: 10px; text-align: center; color: #666;"></div>
                    </div>
                    '''
                    ui.html(upload_html_risk)

                # 风险金延伸
                with ui.column().classes('w-1/2'):
                    ui.label('⚙️ 人力风险金延伸').classes('text-base font-bold')

                    upload_html_risk_ext = '''
                    <div style="padding: 20px; border: 2px dashed #9C27B0; border-radius: 8px; background-color: #f3e5f5;">
                        <input type="file" id="riskExtFile" accept=".xlsx,.xls" style="display: none;">
                        <button id="riskExtChooseBtn" style="width: 100%; padding: 12px; background-color: #FF9800; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; margin-bottom: 10px;">
                            📁 选择文件
                        </button>
                        <button id="riskExtUploadBtn" style="width: 100%; padding: 12px; background-color: #9C27B0; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold;">
                            📤 上传风险金延伸
                        </button>
                        <div id="riskExtFileName" style="margin-top: 10px; text-align: center; font-weight: bold; color: #9C27B0;"></div>
                        <div id="riskExtStatus" style="margin-top: 10px; text-align: center; color: #666;"></div>
                    </div>
                    '''
                    ui.html(upload_html_risk_ext)

            def init_uploaders():
                ui.run_javascript('''
    
                function createUploader(prefix, apiUrl) {
    
                    const file = document.getElementById(prefix + "File");
                    const chooseBtn = document.getElementById(prefix + "ChooseBtn");
                    const uploadBtn = document.getElementById(prefix + "UploadBtn");
                    const fileName = document.getElementById(prefix + "FileName");
                    const status = document.getElementById(prefix + "Status");
    
                    if (!file) return;
    
                    chooseBtn.onclick = () => file.click();
    
                    file.onchange = function () {
                        if (this.files[0]) {
                            fileName.textContent = '✅ 已选择: ' + this.files[0].name;
                        }
                    };
    
                    uploadBtn.onclick = async function () {
    
                        const selected = file.files[0];
    
                        if (!selected) {
                            status.textContent = '❌ 请先选择文件';
                            status.style.color = 'red';
                            return;
                        }
    
                        status.textContent = '📥 上传中...';
                        status.style.color = '#666';
                        uploadBtn.disabled = true;
    
                        const formData = new FormData();
                        formData.append('file', selected);
    
                        try {
    
                            const response = await fetch(apiUrl, {
                                method: 'POST',
                                body: formData
                            });
    
                            const data = await response.json();
    
                            if (data.success) {
    
                                status.textContent = '✅ ' + data.message;
                                status.style.color = 'green';
    
                                fileName.textContent = '';
                                file.value = '';
    
                                uploadBtn.disabled = false;
    
                                setTimeout(() => location.reload(), 2000);
    
                            } else {
    
                                status.textContent = '❌ ' + data.message;
                                status.style.color = 'red';
                                uploadBtn.disabled = false;
    
                            }
    
                        } catch (error) {
    
                            status.textContent = '❌ 上传失败: ' + error.message;
                            status.style.color = 'red';
                            uploadBtn.disabled = false;
    
                        }
    
                    };
    
                }
    
                createUploader("legalDetail", "/upload_legal_economic_detail");
                createUploader("acct", "/upload_legal_accountability");
                createUploader("riskFund", "/upload_hr_risk_fund");
                createUploader("riskExt", "/upload_hr_risk_fund_extended");
    
                ''')

            ui.timer(0.1, init_uploaders, once=True)  # 0.1秒后执行一次

        # ========== Tab 5: 全量导出 ==========
        with ui.tab_panel('tab5'):
            ui.label('📥 导出所有6张表').classes('text-h6')

            def export_all():
                conn = get_conn()

                tables = [
                    ('hr_payroll', '人力工资表'),
                    ('hr_roster', '人力花名册'),
                    ('hr_risk_fund', '人力风险金'),
                    ('hr_risk_fund_extended', '人力风险金延伸'),
                    ('legal_economic_detail', '法律经济处理明细表'),
                    ('legal_accountability', '法律问责办台账'),
                ]

                with pd.ExcelWriter('全表数据导出.xlsx', engine='openpyxl') as writer:
                    for table_name, sheet_name in tables:
                        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
                        # 删除数据库自增ID
                        df = df.drop(columns=['id'], errors='ignore')
                        # 修改为中文表头
                        if table_name in COLUMN_MAP:
                            df.rename(columns=COLUMN_MAP[table_name], inplace=True)
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        #Excel 默认列宽很窄，可以自动调整：
                        for column in df.columns:
                            writer.sheets[sheet_name].column_dimensions[column].width = 18

                conn.close()
                ui.download('全表数据导出.xlsx')

            ui.button('📥 一键导出全部6张表', on_click=export_all).classes('bg-green-500 text-white w-full')

            ui.separator()

            with ui.row().classes('gap-2'):
                def export_payroll():
                    conn = get_conn()
                    df = pd.read_sql("SELECT * FROM hr_payroll", conn)
                    conn.close()
                    # 删除数据库自增ID
                    df = df.drop(columns=['id'], errors='ignore')
                    # 中文表头
                    df.rename(columns=COLUMN_MAP["hr_payroll"], inplace=True)
                    df.to_excel('人力工资表.xlsx', index=False)
                    ui.download('人力工资表.xlsx')

                def export_roster():
                    conn = get_conn()
                    df = pd.read_sql("SELECT * FROM hr_roster", conn)
                    conn.close()
                    # 删除数据库自增ID
                    df = df.drop(columns=['id'], errors='ignore')
                    # 中文表头
                    df.rename(columns=COLUMN_MAP["hr_roster"], inplace=True)
                    df.to_excel('人力花名册.xlsx', index=False)
                    ui.download('人力花名册.xlsx')

                def export_risk_fund():
                    conn = get_conn()
                    df = pd.read_sql("SELECT * FROM hr_risk_fund", conn)
                    conn.close()
                    # 删除数据库自增ID
                    df = df.drop(columns=['id'], errors='ignore')
                    # 中文表头
                    df.rename(columns=COLUMN_MAP["hr_risk_fund"], inplace=True)
                    df.to_excel('人力风险金.xlsx', index=False)
                    ui.download('人力风险金.xlsx')

                def export_risk_ext():
                    conn = get_conn()
                    df = pd.read_sql("SELECT * FROM hr_risk_fund_extended", conn)
                    conn.close()
                    # 删除数据库自增ID
                    df = df.drop(columns=['id'], errors='ignore')
                    # 中文表头
                    df.rename(columns=COLUMN_MAP["hr_risk_fund_extended"], inplace=True)
                    df.to_excel('人力风险金延伸.xlsx', index=False)
                    ui.download('人力风险金延伸.xlsx')

                def export_econ():
                    conn = get_conn()
                    df = pd.read_sql("SELECT * FROM legal_economic_detail", conn)
                    conn.close()
                    # 删除数据库自增ID
                    df = df.drop(columns=['id'], errors='ignore')
                    # 中文表头
                    df.rename(columns=COLUMN_MAP["legal_economic_detail"], inplace=True)
                    df.to_excel('法律经济处理明细表.xlsx', index=False)
                    ui.download('法律经济处理明细表.xlsx')

                def export_acct():
                    conn = get_conn()
                    df = pd.read_sql("SELECT * FROM legal_accountability", conn)
                    conn.close()
                    # 删除数据库自增ID
                    df = df.drop(columns=['id'], errors='ignore')
                    # 中文表头
                    df.rename(columns=COLUMN_MAP["legal_accountability"], inplace=True)
                    df.to_excel('法律问责办台账.xlsx', index=False)
                    ui.download('法律问责办台账.xlsx')

                ui.button('工资表', on_click=export_payroll).classes('bg-blue-500 text-white flex-1')
                ui.button('花名册', on_click=export_roster).classes('bg-blue-500 text-white flex-1')
                ui.button('风险金', on_click=export_risk_fund).classes('bg-blue-500 text-white flex-1')
                ui.button('风险金延伸', on_click=export_risk_ext).classes('bg-blue-500 text-white flex-1')
                ui.button('经济处理', on_click=export_econ).classes('bg-blue-500 text-white flex-1')
                ui.button('问责办台账', on_click=export_acct).classes('bg-blue-500 text-white flex-1')

        # ========== Tab 6: 系统信息 ==========
        with ui.tab_panel('tab6'):
            ui.markdown('''
            ## 📋 系统说明

            这是一个**6表协作系统**，用于法律部门和人力资源部门的财务合规管理。

            ### 🔄 完整工作流程

            **第1阶段：基础导入**
            - 人力部导入：工资表 + 花名册

            **第2阶段：自动计算**
            - 系统自动计算：风险金、风险金延伸、��济处理明细(v1)

            **第3阶段：法律部处理**
            - 法律部下载经济处理明细表(v1)
            - 手工修改：税前、税后
            - 重新上传(v2)

            **第4阶段：其他导入**
            - 人力导入：风险金延伸初始数据
            - 法律导入：问责办台账初始数据

            **第5阶段：最终计算**
            - 系统自动计算所有派生字段

            ### 📊 6张表说明

            | # | 表名 | 来源 | 主键 | 自动计算字段 |
            |---|------|------|------|------------|
            | 1 | 人力工资表 | 人力导入 | 日期+员工号 | ❌ 无 |
            | 2 | 人力花名册 | 人力导入 | 员工号 | ❌ 无 |
            | 3 | 人力风险金 | 自动计算 | 员工号+年份 | ✅ 12个月+累计 |
            | 4 | 人力风险金延伸 | 人力导入+计算 | 日期+员工号 | ✅ 多年份余额+返还 |
            | 5 | 法律经济处理明细 | 法律导入(二次) | 版本+日期+员工号+文号 | ✅ 核算金额 |
            | 6 | 法律问责办台账 | 法律导入+计算 | 日期+员工号 | ✅ 执行金额+累计 |

            ### ⚙️ 计算顺序（重要！）

            1. ✅ 人力风险金（基于工资表）
            2. ✅ 经济处理明细v1（基于工资表）
            3. ✅ 人力风险金延伸（基于风险金+问责台账）
            4. ✅ 问责办台账（基于工资表+花名册+风险金延伸）

            ### 🔗 表间关联规则

            **工资表 → 风险金**
            - 关联条件：员工号 + 年份
            - 提取字段：计提延期薪酬（12个月）

            **工资表 → 经济处理明细**
            - 关联条件：员工号 + 年份
            - 计算公式：核算金额 = (应发合计汇总 / 12) × 系数

            **工资表 → 问责台账**
            - 关联条件：员工号 + 执行日期人力用
            - 提取字段：绩效调整、税后问责

            **花名册 → 问责台账**
            - 关联条件：员工号
            - 提取字段：政治面貌

            **风险金延伸 → 问责台账**
            - 关联条件：员工号 + 执行日期
            - 提取字段：问责办实际执行税前

            ### ⚠️ 注意事项

            1. **日期格式**：所有日期必须是 `YYYYMM` 格式（如 202501）
            2. **关联失败**：找不到关联数据时，相关字段留空
            3. **累计计算**：按执行日期从小到大累计求和
            4. **版本控制**：经济处理明细表有v1和v2两个版本

            ### 📞 技术支持

            如有问题，请检查：
            - ✅ 文件格式是否为 .xlsx
            - ✅ 日期格式是否为 YYYYMM
            - ✅ 主键字段是否有重复
            - ✅ 导入顺序是否正确
            ''')


# ==================== 应用启动 ====================

if __name__ == '__main__':
    def open_browser():
        import time
        time.sleep(1)
        webbrowser.open('http://127.0.0.1:8080')


    print("=" * 60)
    print("🚀 6表协作系统启动中...")
    print("📍 地址: http://127.0.0.1:8080")
    print("=" * 60)
    print()

    browser_thread = threading.Thread(target=open_browser, daemon=True)
    browser_thread.start()

    ui.run(host='0.0.0.0', port=8080, reload=False, show=False)