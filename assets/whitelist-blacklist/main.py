import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime, timedelta, timezone
import os
from urllib.parse import urlparse, quote, unquote
import socket
import subprocess

timestart = datetime.now()
# 数据源UA
USER_AGENT_URL = "PostmanRuntime-ApipostRuntime/1.1.0"
# 直播数据UA
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
# 数据源超时
TIMEOUT_FETCH = 5
# 直播数据超时
TIMEOUT_CHECK = 5
# 请求线程数
MAX_WORKERS = 50
blacklist_dict = {}
urls_all_lines = []
url_statistics = []

def read_txt_to_array(file_name):
    try:
        with open(file_name, 'r', encoding='utf-8') as file:
            return [line.strip() for line in file.readlines() if line.strip()]
    except FileNotFoundError:
        print(f"File '{file_name}' not found.")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def read_txt_file(file_path):
    skip_strings = ['#genre#', '#EXTINF:-1', '"ext"']
    required_strings = ['://']
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return [
                line.strip() for line in file
                if not any(skip_str in line for skip_str in skip_strings)
                and all(req_str in line for req_str in required_strings)
            ]
    except Exception as e:
        print(f"Read file error {file_path}: {e}")
        return []

def get_host_from_url(url):
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc
    if host.startswith('[') and host.endswith(']'):
        host = host[1:-1]
    # 处理端口号
    if ':' in host and not host.count(':') > 1:  # 排除IPv6地址中的冒号
        host = host.split(':')[0]
    return host

def is_ipv6_address(ip):
    """判断是否为IPv6地址"""
    try:
        socket.inet_pton(socket.AF_INET6, ip)
        return True
    except (socket.error, ValueError):
        return False

def is_ipv4_address(ip):
    """判断是否为IPv4地址"""
    try:
        socket.inet_pton(socket.AF_INET, ip)
        return True
    except (socket.error, ValueError):
        return False

def record_host(host):
    if not host:
        return
    blacklist_dict[host] = blacklist_dict.get(host, 0) + 1

def force_ip_version_resolver(ip_version):
    """生成指定IP版本的解析器"""
    def resolver(host, port=80, family=0, type=0, proto=0, flags=0):
        addr_family = socket.AF_INET6 if ip_version == 6 else socket.AF_INET
        addrs = socket.getaddrinfo(host, port, addr_family, type, proto, flags)
        return addrs
    return resolver

def check_http_url_with_ip_version(url, timeout, ip_version):
    """指定IP版本检测HTTP/HTTPS链接"""
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "*/*",
                "Connection": "close"
            }
        )

        # 保存原始解析器
        original_getaddrinfo = socket.getaddrinfo
        try:
            # 强制使用指定的IP版本
            socket.getaddrinfo = force_ip_version_resolver(ip_version)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return 200 <= resp.status < 300
        finally:
            # 恢复原始解析器
            socket.getaddrinfo = original_getaddrinfo

    except Exception:
        return False

def check_http_url(url, timeout):
    """优化的HTTP/HTTPS链接检测逻辑：仅使用对应IP版本检测，不降级"""
    try:
        # 获取主机地址
        host = get_host_from_url(url)

        # 1. 判断IP类型
        if is_ipv6_address(host):
            # URL是IPv6地址，仅检测IPv6，失败不降级
            return check_http_url_with_ip_version(url, timeout, 6)

        elif is_ipv4_address(host):
            # URL是IPv4地址，仅检测IPv4，失败不降级
            return check_http_url_with_ip_version(url, timeout, 4)

        else:
            # 域名形式，先尝试IPv4（无降级）
            return check_http_url_with_ip_version(url, timeout, 4)

    except urllib.error.HTTPError as e:
        return False
    except (urllib.error.URLError, socket.timeout, ConnectionResetError):
        return None
    except Exception:
        return False

def check_rtmp_rtsp_url(url, timeout):
    """优化的RTMP/RTSP检测逻辑"""
    try:
        # 使用ffprobe检测，增加详细输出便于调试，延长超时
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-timeout', f'{timeout * 1000000}', url],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=timeout
        )
        # ffprobe返回码为0表示链接有效
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        # ffprobe未找到或超时，返回None表示未知
        return None
    except Exception:
        return False

def check_rtp_url(url, timeout):
    """优化的RTP检测逻辑：仅使用对应IP版本检测，不降级"""
    try:
        parsed = urlparse(url)
        host, port = parsed.hostname, parsed.port
        if not host or not port:
            return False

        # 判断IP类型，仅使用对应类型检测
        if is_ipv6_address(host):
            # IPv6地址，仅用IPv6 socket
            with socket.socket(socket.AF_INET6, socket.SOCK_DGRAM) as s:
                s.settimeout(timeout)
                s.connect((host, port))
                return True
        else:
            # IPv4地址或域名，仅用IPv4 socket
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.settimeout(timeout)
                s.connect((host, port))
                return True
    except socket.timeout:
        return None
    except Exception:
        return False

def check_p3p_url(url, timeout):
    try:
        parsed = urlparse(url)
        host, port = parsed.hostname, parsed.port or 80
        path = parsed.path or "/"
        if not host or not port:
            return False

        # 判断IP类型，使用对应socket类型
        if is_ipv6_address(host):
            s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        else:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        with s:
            s.settimeout(timeout)
            s.connect((host, port))
            request = (
                f"GET {path} P3P/1.0\r\n"
                f"Host: {host}\r\n"
                f"User-Agent: {USER_AGENT}\r\n"
                f"Connection: close\r\n\r\n"
            )
            s.sendall(request.encode())
            return True
    except Exception:
        return False

def check_p2p_url(url, timeout):
    try:
        parsed = urlparse(url)
        host, port, path = parsed.hostname, parsed.port, parsed.path
        if not host or not port or not path:
            return False

        # 判断IP类型，使用对应socket类型
        if is_ipv6_address(host):
            s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        else:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        with s:
            s.settimeout(timeout)
            s.connect((host, port))
            request = f"YOUR_CUSTOM_REQUEST {path}\r\nHost: {host}\r\n\r\n"
            s.sendall(request.encode())
            return b"SOME_EXPECTED_RESPONSE" in s.recv(1024)
    except Exception:
        return False

def check_url(url, timeout=TIMEOUT_CHECK):
    """重构的链接检测函数：仅使用对应IP版本检测，不降级"""
    try:
        # 统一URL编码处理
        encoded_url = quote(unquote(url), safe=':/?&=')
        start_time = time.time()

        is_valid = None
        if url.startswith(("http", "https")):
            is_valid = check_http_url(encoded_url, timeout)
        elif url.startswith(("rtmp", "rtsp")):
            is_valid = check_rtmp_rtsp_url(encoded_url, timeout)
        elif url.startswith("rtp"):
            is_valid = check_rtp_url(encoded_url, timeout)
        elif url.startswith("p3p"):
            is_valid = check_p3p_url(encoded_url, timeout)
        elif url.startswith("p2p"):
            is_valid = check_p2p_url(encoded_url, timeout)
        else:
            # 未知协议，标记为失效
            is_valid = False

        # 处理检测结果：
        # - True: 有效
        # - False: 确实失效
        # - None: 检测超时/未知，暂判定为有效（避免误杀）
        real_elapsed = (time.time() - start_time) * 1000

        if is_valid is True:
            return real_elapsed, True
        elif is_valid is False:
            record_host(get_host_from_url(url))
            return None, False
        else:
            # 未知状态，判定为有效，避免误杀可用链接
            return real_elapsed, True

    except Exception as e:
        # 捕获未预期的异常，记录但不直接判定失效
        print(f"Check URL error {url}: {str(e)[:50]}")
        return None, False

def is_m3u_content(text):
    return text.strip().startswith("#EXTM3U") if text else False

def convert_m3u_to_txt(m3u_content):
    lines = [line.strip() for line in m3u_content.split('\n') if line.strip()]
    txt_lines, channel_name = [], ""
    for line in lines:
        if line.startswith("#EXTINF"):
            channel_name = line.split(',')[-1].strip()
        elif line.startswith(("http", "rtmp", "rtsp", "p3p", "p2p", "rtp")) and channel_name:
            txt_lines.append(f"{channel_name},{line}")
    return txt_lines

def process_url(url):
    try:
        encoded_url = quote(unquote(url), safe=':/?&=')
        req = urllib.request.Request(encoded_url, headers={"User-Agent": USER_AGENT_URL})
        with urllib.request.urlopen(req, timeout=TIMEOUT_FETCH) as resp:
            text = resp.read().decode('utf-8', errors='replace')
            if is_m3u_content(text):
                m3u_lines = convert_m3u_to_txt(text)
                url_statistics.append(f"{len(m3u_lines)},{url.strip()}")
                urls_all_lines.extend(m3u_lines)
            else:
                valid_lines = [
                    line.strip() for line in text.split('\n')
                    if line.strip() and "#genre#" not in line and "," in line and "://" in line
                ]
                url_statistics.append(f"{len(valid_lines)},{url.strip()}")
                urls_all_lines.extend(valid_lines)
    except Exception as e:
        print(f"Process URL error {url}: {e}")

def split_url(lines):
    newlines = []
    for line in lines:
        if "," not in line or "://" not in line:
            continue
        channel_name, channel_url = line.split(',', 1)
        if "#" not in channel_url:
            newlines.append(line)
        else:
            for url in channel_url.split('#'):
                url = url.strip()
                if "://" in url:
                    newlines.append(f"{channel_name},{url}")
    return newlines

def clean_url(lines):
    newlines = []
    for line in lines:
        if "," in line and "://" in line:
            dollar_idx = line.rfind('$')
            newlines.append(line[:dollar_idx] if dollar_idx != -1 else line)
    return newlines

def remove_duplicates_url(lines):
    url_set, newlines = set(), []
    for line in lines:
        if "," in line and "://" in line:
            _, url = line.split(',', 1)
            url = url.strip()
            if url not in url_set:
                url_set.add(url)
                newlines.append(line)
    return newlines

def process_line(line, whitelist):
    if "#genre#" in line or "://" not in line or not line.strip():
        return None, None
    parts = line.split(',', 1)
    if len(parts) != 2:
        return None, None
    name, url = parts
    url = url.strip()

    elapsed_time, is_valid = check_url(url)

    # 白名单链接强制标记为有效
    if url in whitelist:
        return (elapsed_time if elapsed_time else 0.01, line)
    else:
        return (elapsed_time, line) if is_valid else (None, line)

def process_urls_multithreaded(lines, whitelist, max_workers=MAX_WORKERS):
    successlist, blacklist = [], []
    total = len(lines)
    processed = 0
    ok = 0
    ng = 0

    if not lines:
        return successlist, blacklist

    print(f"\n正在开始检测，总计：{total} 条")
    print("-" * 50)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_line, line, whitelist): line for line in lines}
        for future in as_completed(futures):
            processed += 1
            elapsed, result = future.result()
            if result:
                if elapsed is not None:
                    successlist.append(f"{elapsed:.2f}ms,{result}")
                    ok += 1
                else:
                    blacklist.append(result)
                    ng += 1

            # 实时进度
            print(f"\r进度：{processed}/{total} | 成功：{ok} | 失败：{ng}", end="")

    # 按响应时间排序
    successlist.sort(key=lambda x: float(x.split(',')[0].replace('ms', '')))
    blacklist.sort()
    print("\n" + "-" * 50)
    return successlist, blacklist

def write_list(file_path, data_list):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(data_list))
        print(f"File generated: {file_path}")
    except Exception as e:
        print(f"Write file error {file_path}: {e}")

def remove_prefix_from_lines(lines):
    result = []
    for line in lines:
        if "," in line and "://" in line and "ms," in line:
            result.append(",".join(line.split(",")[1:]))
    return result

def get_file_paths():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    parent2_dir = os.path.dirname(parent_dir)
    return {
        "urls": os.path.join(parent_dir, 'urls.txt'),
        "live": os.path.join(parent2_dir, 'live.txt'),
        "blacklist_auto": os.path.join(current_dir, 'blacklist_auto.txt'),
        "others": os.path.join(parent2_dir, 'others.txt'),
        "whitelist_manual": os.path.join(current_dir, 'whitelist_manual.txt'),
        "whitelist_auto": os.path.join(current_dir, 'whitelist_auto.txt'),
        "whitelist_auto_tv": os.path.join(current_dir, 'whitelist_auto_tv.txt')
    }

if __name__ == "__main__":
    file_paths = get_file_paths()
    remote_urls = read_txt_to_array(file_paths["urls"])

    for url in remote_urls:
        if url.startswith("http"):
            print(f"Process remote URL: {url}")
            process_url(url)

    lines_whitelist = read_txt_file(file_paths["whitelist_manual"])
    lines = urls_all_lines

    print(f"Original data count: {len(lines)}")
    lines = split_url(lines)
    lines_whitelist = split_url(lines_whitelist)
    lines = clean_url(lines)
    lines_whitelist = clean_url(lines_whitelist)
    lines = remove_duplicates_url(lines)
    lines_whitelist = remove_duplicates_url(lines_whitelist)
    clean_count = len(lines)
    print(f"Cleaned data count: {clean_count}")

    whitelist_set = set()
    for line in lines_whitelist:
        if "," in line and "://" in line:
            _, url = line.split(',', 1)
            whitelist_set.add(url.strip())
    print(f"Whitelist URL count: {len(whitelist_set)}")

    successlist, blacklist = process_urls_multithreaded(lines, whitelist_set)
    ok_count, ng_count = len(successlist), len(blacklist)
    print(f"Check done - Success: {ok_count}, Failed: {ng_count}")

    bj_time = datetime.now(timezone.utc) + timedelta(hours=8)
    version = f"{bj_time.strftime('%Y%m%d %H:%M')},url"
    success_tv = remove_prefix_from_lines(successlist)

    success_output = [
        "更新时间,#genre#", version, "",
        "RespoTime,whitelist,#genre#"
    ] + successlist
    success_tv_output = [
        "更新时间,#genre#", version, "",
        "whitelist,#genre#"
    ] + success_tv
    black_output = [
        "更新时间,#genre#", version, "",
        "blacklist,#genre#"
    ] + blacklist

    write_list(file_paths["whitelist_auto"], success_output)
    write_list(file_paths["whitelist_auto_tv"], success_tv_output)
    write_list(file_paths["blacklist_auto"], black_output)

    end_time = datetime.now()
    elapsed = end_time - timestart
    mins, secs = int(elapsed.total_seconds() // 60), int(elapsed.total_seconds() % 60)
    print("="*50)
    print(f"Elapsed time: {mins} min {secs} sec")
    print(f"Original count: {len(urls_all_lines)}")
    print(f"Cleaned count: {clean_count}")
    print(f"Success count: {ok_count}")
    print(f"Failed count: {ng_count}")
    print("="*50)

    for stat in url_statistics:
        print(stat)
