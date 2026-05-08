import time
import json
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    NoSuchElementException,
    WebDriverException
)

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import os
import sys
from datetime import datetime


# =========================
# driver path
# =========================
def get_driver_path():
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, 'chromedriver.exe')
    return os.path.join(os.getcwd(), 'chromedriver.exe')


# =========================
# 日志辅助函数
# =========================
def print_header(task_name):
    """打印任务开始头"""
    print("\n" + "="*70)
    print(f"📋 开始执行任务: {task_name}")
    print(f"⏰ 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70 + "\n")


def print_page_header(page_index, page_url):
    """打印页面开始头"""
    print(f"\n📄 【第 {page_index + 1} 个页面】")
    print(f"🔗 URL: {page_url}")
    print("-" * 70)


def print_step_start(step, step_index):
    """打印步骤开始"""
    page_index = step.get('page_index', 0)
    action = step.get('action', 'unknown')
    name = step.get('name', 'Unknown')
    
    action_emoji = "⌨️ " if action == "input" else "🖱️ "
    print(f"{action_emoji} [{page_index}页-步{step_index}] 执行: {name}")


def print_step_success(value=""):
    """打印步骤成功"""
    if value:
        print(f"   ✅ 成功（输入值: {value}）")
    else:
        print(f"   ✅ 成功")


def print_step_failed():
    """打印步骤失败"""
    print(f"   ❌ 失败")


# =========================
# 等待元素（稳定核心）
# =========================
def find_element_safe(driver, xpath, iframe_path=None, timeout=10):

    try:
        driver.switch_to.default_content()

        # iframe支持（如果有）
        if iframe_path:
            try:
                if "iframe[" in iframe_path:
                    indexes = iframe_path.split(">")
                    for item in indexes:
                        idx = int(item.replace("iframe[", "").replace("]", ""))
                        iframe = driver.find_elements(By.TAG_NAME, "iframe")[idx]
                        driver.switch_to.frame(iframe)
            except Exception as e:
                print(f"   ⚠️  iframe切换异常: {e}")
                pass

        wait = WebDriverWait(driver, timeout)

        element = wait.until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )

        return element

    except TimeoutException:
        return None


# =========================
# click（防DOM变化）
# =========================
def safe_click(driver, element):

    try:
        element.click()
        return True

    except Exception:
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            return False


# =========================
# input（防stale）
# =========================
def safe_input(driver, element, value):

    try:
        element.clear()
    except Exception:
        pass

    try:
        element.send_keys(value)
        return True
    except StaleElementReferenceException:
        return False
    except Exception:
        try:
            driver.execute_script(
                "arguments[0].value=arguments[1];",
                element,
                value
            )
            return True
        except Exception:
            return False


# =========================
# 等待页面加载
# =========================
def wait_page_load(driver, timeout=10):
    """等待页面完全加载"""
    try:
        WebDriverWait(driver, timeout).until(
            lambda driver: driver.execute_script('return document.readyState') == 'complete'
        )
        time.sleep(1)  # 额外等待1秒确保JS执行
        return True
    except Exception as e:
        print(f"   ⚠️  页面加载超时: {e}")
        return True  # 继续执行


# =========================
# ⭐ 等待指定元素消失（用于判断加载/处理完成）
# =========================
def wait_element_disappear(driver, xpath, iframe_path=None, timeout=30):
    """
    等待某个元素消失（如加载框、弹窗）
    使用场景：报告加载完成、导出完成等
    """
    try:
        driver.switch_to.default_content()
        
        if iframe_path:
            try:
                if "iframe[" in iframe_path:
                    indexes = iframe_path.split(">")
                    for item in indexes:
                        idx = int(item.replace("iframe[", "").replace("]", ""))
                        iframe = driver.find_elements(By.TAG_NAME, "iframe")[idx]
                        driver.switch_to.frame(iframe)
            except Exception as e:
                print(f"   ⚠️  iframe切换异常: {e}")
                pass
        
        wait = WebDriverWait(driver, timeout)
        wait.until(
            EC.invisibility_of_element_located((By.XPATH, xpath))
        )
        print(f"   ✅ 等待完成（元素已消失）")
        return True
        
    except TimeoutException:
        print(f"   ⚠️  等待超时，元素仍未消失")
        return False
    except Exception as e:
        print(f"   ⚠️  等待异常: {e}")
        return False


# =========================
# ⭐ 等待指定元素出现（用于判断弹窗/结果出现）
# =========================
def wait_element_appear(driver, xpath, iframe_path=None, timeout=30):
    """
    等待某个元素出现（如结果提示、确认框）
    使用场景：导出完成提示、成功弹窗等
    """
    try:
        driver.switch_to.default_content()
        
        if iframe_path:
            try:
                if "iframe[" in iframe_path:
                    indexes = iframe_path.split(">")
                    for item in indexes:
                        idx = int(item.replace("iframe[", "").replace("]", ""))
                        iframe = driver.find_elements(By.TAG_NAME, "iframe")[idx]
                        driver.switch_to.frame(iframe)
            except Exception as e:
                print(f"   ⚠️  iframe切换异常: {e}")
                pass
        
        wait = WebDriverWait(driver, timeout)
        element = wait.until(
            EC.visibility_of_element_located((By.XPATH, xpath))
        )
        print(f"   ✅ 等待完成（元素已出现）")
        return True
        
    except TimeoutException:
        print(f"   ⚠️  等待超时，元素未出现")
        return False
    except Exception as e:
        print(f"   ⚠️  等待异常: {e}")
        return False


# =========================
# 执行单步（核心稳定逻辑）
# =========================
def execute_step(driver, step, step_index, next_step=None):

    xpath = step.get("xpath")
    action = step.get("action")
    value = step.get("value", "")
    iframe = step.get("iframe", "")
    name = step.get("name", "Unknown")

    print_step_start(step, step_index)

    for retry in range(3):  # ⭐ 关键：重试3次

        try:

            element = find_element_safe(
                driver,
                xpath,
                iframe,
                timeout=8
            )

            if not element:
                if retry < 2:
                    print(f"   ⚠️  未找到元素，重试 ({retry + 1}/3)...")
                    time.sleep(1.5)
                    continue
                else:
                    print_step_failed()
                    return False

            if action == "input":
                ok = safe_input(driver, element, value)
                if ok:
                    print_step_success(value if value else "[空值]")
                    return True
                else:
                    if retry < 2:
                        print(f"   ⚠️  输入失败���重试 ({retry + 1}/3)...")
                        time.sleep(1)
                        continue
                    else:
                        print_step_failed()
                        return False
            else:
                ok = safe_click(driver, element)
                if ok:
                    print_step_success()
                    
                    # ⭐ 核心优化：点击后智能判断等待
                    # 根据步骤名称判断是否需要特殊等待
                    if _should_wait_after_click(name, next_step):
                        _handle_special_wait(driver, name, next_step, iframe)
                    
                    return True
                else:
                    if retry < 2:
                        print(f"   ⚠️  点击失败，重试 ({retry + 1}/3)...")
                        time.sleep(1)
                        continue
                    else:
                        print_step_failed()
                        return False

        except WebDriverException as e:

            # ⭐ window关闭保护
            if "no such window" in str(e):
                print(f"   ❌ 浏览器窗口已关闭，终止任务")
                return False

            if retry < 2:
                print(f"   ⚠️  WebDriver异常，重试 ({retry + 1}/3)...")
                time.sleep(1.5)
            else:
                print_step_failed()
                return False

        except Exception as e:
            if retry < 2:
                print(f"   ⚠️  异常: {str(e)[:50]}，重试 ({retry + 1}/3)...")
                time.sleep(1)
            else:
                print(f"   ❌ 异常: {e}")
                return False

    print_step_failed()
    return False


# =========================
# ⭐ 判断是否需要特殊等待
# =========================
def _should_wait_after_click(step_name, next_step):
    """判断点击后是否需要等待加载/完成"""
    # 需要等待加载完成的关键词
    load_keywords = ["信审流程汇总", "产品运营", "报告", "信息", "数据"]
    
    # 需要等待导出完成的关键词
    export_keywords = ["导出", "下载", "确定"]
    
    step_name_lower = step_name.lower()
    
    for keyword in load_keywords:
        if keyword in step_name_lower:
            return True
    
    for keyword in export_keywords:
        if keyword in step_name_lower:
            return True
    
    return False


# =========================
# ⭐ 处理特殊等待逻辑
# =========================
def _handle_special_wait(driver, step_name, next_step, iframe):
    """根据步骤名称处理不同的等待逻辑"""
    
    # 情况1：加载报告（等待加载框消失）
    if "信审流程汇总" in step_name or "产品运营" in step_name:
        print(f"   ⏳ 等待报告加载中...")
        
        # 常见的加载框/加载指示器 XPath
        loading_xpaths = [
            "//*[contains(@class, 'loading')]",  # class包含loading
            "//*[contains(@class, 'spinner')]",  # 旋转加载
            "//*[contains(@class, 'progress')]",  # 进度条
            "//div[@class='v-spinner']",  # Vue spinner
            "//*[@role='progressbar']",  # ARIA progressbar
        ]
        
        for loading_xpath in loading_xpaths:
            try:
                if wait_element_disappear(driver, loading_xpath, iframe, timeout=30):
                    print(f"   ✅ 报告加载完成")
                    time.sleep(2)
                    return
            except:
                continue
        
        # 如果没有找到加载框，就等待普通加载
        print(f"   ⏳ 等待页面加载完成...")
        wait_page_load(driver, timeout=20)
    
    # 情况2：导出相关（等待导出框消失和成功提示出现）
    elif "导出" in step_name or "下载" in step_name:
        print(f"   ⏳ 等待导出处理中...")
        
        # 先等待导出框消失（最多等30秒）
        export_dialog_xpaths = [
            "//*[contains(@class, 'dialog')][@style*='display: block']",
            "//*[contains(@class, 'modal')][@style*='display: block']",
            "//div[@role='dialog']",
            "//*[contains(text(), '导出')]//ancestor::div[contains(@class, 'dialog')]",
        ]
        
        # 尝试等待导出框消失
        for dialog_xpath in export_dialog_xpaths:
            try:
                if wait_element_disappear(driver, dialog_xpath, iframe, timeout=30):
                    print(f"   ✅ 导出对话框已关闭")
                    time.sleep(2)
                    return
            except:
                continue
        
        # 如果没有等到对话框消失，检查是否有成功提示
        success_xpaths = [
            "//*[contains(text(), '成功')]",
            "//*[contains(@class, 'success')]",
            "//*[contains(text(), '完成')]",
        ]
        
        for success_xpath in success_xpaths:
            try:
                if wait_element_appear(driver, success_xpath, iframe, timeout=15):
                    print(f"   ✅ 导出成功")
                    time.sleep(2)
                    return
            except:
                continue
        
        # 如果都没有，就等待一个通用的等待时间
        print(f"   ⏳ 等待导出完成（通用等待）...")
        time.sleep(5)


# =========================
# 按页面分组步骤
# =========================
def group_steps_by_page(steps):
    """按 page_index 分组步骤"""
    groups = {}
    for step in steps:
        page_idx = step.get('page_index', 0)
        if page_idx not in groups:
            groups[page_idx] = []
        groups[page_idx].append(step)
    
    # 按页面索引排序
    return {k: groups[k] for k in sorted(groups.keys())}


# =========================
# 主执行入口（稳定版）
# =========================
def run_task(config_path):

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    task_name = config.get("name", "Unknown")
    print_header(task_name)

    driver = webdriver.Chrome(
        service=Service(get_driver_path())
    )

    try:
        print(f"⏳ 访问登录页: {config['login_url']}\n")
        driver.get(config["login_url"])
        wait_page_load(driver)

        steps = config.get("steps", [])
        
        if not steps:
            print("❌ 没有步骤可执行")
            return

        # 按页面分组
        step_groups = group_steps_by_page(steps)
        
        current_page_index = None
        step_counter = 1

        for i, step in enumerate(steps):
            page_index = step.get('page_index', 0)
            
            # 新页面开始
            if current_page_index != page_index:
                current_page_index = page_index
                page_url = step.get('url', driver.current_url)
                print_page_header(page_index, page_url)

            # 获取下一步（用于某些判断逻辑）
            next_step = steps[i + 1] if i + 1 < len(steps) else None

            # 执行步骤
            ok = execute_step(driver, step, step_counter, next_step)

            if not ok:
                print("\n" + "="*70)
                print(f"❌ 执行失败，停止任务")
                print("="*70 + "\n")
                break

            step_counter += 1
            time.sleep(1.5)  # 步骤间隔

        else:
            # 所有步骤执行完成
            print("\n" + "="*70)
            print(f"✅ 任务执行完成")
            print("="*70 + "\n")

    except Exception as e:
        print("\n" + "="*70)
        print(f"❌ 任务异常: {e}")
        print("="*70 + "\n")

    finally:
        try:
            driver.quit()
        except:
            pass
