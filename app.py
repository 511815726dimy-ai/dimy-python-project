import os
import json
import threading
import webbrowser
import sys
import socket
from flask import Flask
from flask import render_template
from flask import request
from flask import jsonify
import time
from flask_cors import CORS

from element_picker import start_picker
from runner import run_task

def resource_path(relative_path):

    try:

        base_path = sys._MEIPASS

    except Exception:

        base_path = os.path.abspath(".")

    return os.path.join(
        base_path,
        relative_path
    )

app = Flask(

    __name__,

    template_folder=resource_path(
        "templates"
    )

)
os.makedirs("configs", exist_ok=True)

os.makedirs("downloads", exist_ok=True)

os.makedirs("logs", exist_ok=True)


CORS(app)

# =========================
# 临时存储录制结果
# =========================
xpath_store = {}


# =========================
# 首页
# =========================
@app.route("/")
def index():

    tasks = []

    if os.path.exists("configs"):

        for file in os.listdir("configs"):

            if file.endswith(".json"):

                tasks.append(
                    file.replace(".json", "")
                )

    return render_template(
        "index.html",
        tasks=tasks
    )


# =========================
# 创建任务
# =========================
@app.route("/create", methods=["GET", "POST"])
def create():

    if request.method == "POST":

        data = request.get_json()

        config = {

            "name": data.get("name"),

            "login_url": data.get("login_url"),

            "download_path": data.get("download_path"),

            "steps": data.get("steps", [])

        }

        filename = f'configs/{config["name"]}.json'

        with open(
            filename,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                config,
                f,
                ensure_ascii=False,
                indent=2
            )

        return {
            "status": "success"
        }

    return render_template("create.html")


# =========================
# 启动录制器
# =========================
def launch_picker(url, port):

    try:
        start_picker(url, port)
    except Exception as e:
        print("picker异常:", e)


@app.route("/start_picker")
def start_picker_route():

    url = request.args.get("url")

    threading.Thread(
        target=launch_picker,
        args=(url, app.config["PORT"]),
        daemon=True   # 关键
    ).start()

    return "录制器已启动"


# =========================
# 保存 XPath（已启用去重）
# =========================
@app.route("/save_xpath", methods=["POST", "OPTIONS"])
def save_xpath():

    # 处理 CORS 预检请求
    if request.method == "OPTIONS":
        return {
            "status": "ok"
        }

    data = request.get_json()

    print("收到Step:", data)

    # 第一次初始化
    if "steps" not in xpath_store:

        xpath_store["steps"] = []

    # ⭐ 去重检查：防止保存重复步骤
    current_xpath = data.get("xpath", "")
    current_action = data.get("action", "")
    current_name = data.get("name", "")
    current_index = data.get("index", 0)
    
    # 检查是否与最后一个步骤重复
    if xpath_store["steps"]:
        last_step = xpath_store["steps"][-1]
        if (last_step.get("xpath") == current_xpath and
            last_step.get("action") == current_action and
            last_step.get("name") == current_name):
            print(f"⚠️  [后端去重] 忽略重复的步骤: {current_name}")
            return {
                "status": "ok",
                "duplicated": True,
                "message": "步骤重复，已忽略"
            }

    # 添加step
    xpath_store["steps"].append(data)

    xpath_store["steps"].sort(
        key=lambda x: x.get("index", 0)
    )

    print(f"✅ 步骤已保存 (共 {len(xpath_store['steps'])} 个)")

    return {
        "status": "ok",
        "duplicated": False
    }

# =========================
# 获取录制结果
# =========================
@app.route("/get_xpath")
def get_xpath():

    return jsonify(xpath_store)


# =========================
# 清除录制结果（支持重新录制）
# =========================
@app.route("/clear_xpath", methods=["POST"])
def clear_xpath():
    global xpath_store
    
    count = len(xpath_store.get("steps", []))
    xpath_store = {}
    
    print(f"✅ 已清除 {count} 个步骤，可以重新开始录制")
    
    return {
        "status": "ok",
        "cleared_count": count
    }


# =========================
# 执行任务
# =========================
@app.route("/run/<task_name>")
def run(task_name):

    config_path = f"configs/{task_name}.json"

    if not os.path.exists(config_path):

        return "任务不存在"

    threading.Thread(
        target=run_task,
        args=(config_path,)
    ).start()

    return f"任务 {task_name} 开始执行"


# =========================
# 测试页面
# =========================
@app.route("/test_login")
def test_login():

    return render_template(
        "test_login.html"
    )


# =========================
# 自动打开浏览器
# =========================
def open_browser(port):

    webbrowser.open(

        f"http://127.0.0.1:{port}"

    )



def get_free_port():

    s = socket.socket()

    s.bind(("", 0))

    port = s.getsockname()[1]

    s.close()

    return port

# =========================
# 启动
# =========================
if __name__ == "__main__":

    app.config["PORT"] = get_free_port()



    # =========================
    # ⭐ 等待 Flask 真正可访问（改进版）
    # =========================
    def wait_and_open(port):

        print("\n⏳ 等待 Flask 启动完成...")

        for attempt in range(100):  # 最多等10秒

            try:
                s = socket.socket()
                s.settimeout(0.2)
                s.connect(("127.0.0.1", port))
                s.close()
                print(f"✅ Flask 已准备就绪 (第 {attempt + 1} 次尝试)")
                break
            except:
                if attempt % 10 == 0:
                    print(f"   等待中... ({attempt}0ms)")
                time.sleep(0.1)

        # 打开浏览器
        print("🌐 打开浏览器中...\n")
        try:
            webbrowser.open(f"http://127.0.0.1:{port}", new=1)
            time.sleep(1)
            # 再激活一次，确保窗口在前台
            webbrowser.open(f"http://127.0.0.1:{port}", new=0)
        except Exception as e:
            print(f"❌ 浏览器打开失败: {e}\n")
            print(f"请手动打开: http://127.0.0.1:{port}\n")

    # =========================
    # 启动浏览器线程
    # =========================
    browser_thread = threading.Thread(
        target=wait_and_open,
        args=(app.config["PORT"],),
        daemon=True
    )
    browser_thread.start()

    # =========================
    # Flask启动
    # =========================
    print("\n" + "="*70)
    print("🚀 RPA 任务管理系统启动中...")
    print("="*70 + "\n")

    app.run(
        debug=False,
        use_reloader=False,
        port=app.config["PORT"],
        host="127.0.0.1"
    )
