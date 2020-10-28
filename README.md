# 用户个信息爬虫

### 工具:  
- `python` 版本 `>3.0` 
- `pip` 
- `mysql`
- `elasticSearch`

### 脚本方式运行:
- 安装依赖 `pip install -r requirements.txt`
- 运行:  
`python pyspider scheduler -c [config] -m [TASK]`
`python pyspider server -c [config] -m [TASK]`

### 打包运行:
- `pyinstaller -F pyspider.py`
