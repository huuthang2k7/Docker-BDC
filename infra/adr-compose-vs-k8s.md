# ADR: Lựa chọn nền tảng điều phối Container — Docker Compose vs. Kubernetes


## 1. Bối cảnh (Context)

Hệ thống thu thập dữ liệu sự kiện theo thời gian thực gồm 4 thành phần chính: `zookeeper`, `kafka` (`cp-kafka:7.4.0`), `producer` (phát sự kiện JSON với mã định danh duy nhất và `worker` (consumer Python ghi dữ liệu raw xuống định dạng JSONL).

Qua quá trình thử nghiệm thực tế trên hai mô hình hạ tầng — **Lab A (Docker Compose)** và **Lab B (Kubernetes cục bộ qua `k3d`)** — cần đưa ra quyết định kiến trúc rõ ràng về việc sử dụng nền tảng nào cho từng giai đoạn phát triển, kiểm thử và vận hành thực tế (Production), đảm bảo cân bằng giữa tốc độ phát triển, chi phí tài nguyên và độ tin cậy của dữ liệu.

---

## 2. Các yếu tố quyết định

1. **Hiệu quả sử dụng tài nguyên trên máy (Local Resource Footprint):** Môi trường lập trình chạy trên Windows + WSL2 + Docker Desktop với giới hạn RAM chia sẻ.
2. **Tốc độ vòng lặp phát triển:** Thời gian khởi tạo môi trường, build image, đọc log và gỡ lỗi (troubleshooting) phải nhanh gọn khi phát triển tính năng mới.
3. **Tính sẵn sàng của dịch vụ (Service Restart & Self-healing):** Khả năng tự động phát hiện tiến trình treo (qua healthcheck/probes) và tự khôi phục dịch vụ khi container/Pod gặp sự cố hoặc cập nhật phiên bản.
4. **Tính đúng đắn dữ liệu (Data Correctness):** Đảm bảo cơ chế giao nhận *at-least-once* hoạt động an toàn khi dịch vụ dừng/khởi động lại (tắt êm qua `SIGTERM`, gọi `os.fsync()` trước khi `consumer.commit()` thủ công), đồng thời giữ cho worker hoàn toàn phi trạng thái (**stateless**).

---

## 3. Các phương án xem xét (Considered Options)

### Phương án 1: Dùng thuần túy Docker Compose cho mọi môi trường
* **Mô tả:** Đóng gói toàn bộ stack (`zookeeper`, `kafka`, `producer`, `worker`) trong một file `compose.yaml` duy nhất trên mạng bridge `kafka-lab-net`, sử dụng `restart: unless-stopped` và healthcheck cục bộ.
* **Ưu điểm:**
  * Khởi động cực nhanh (**~5–6 giây**), tổng mức tiêu thụ RAM thấp (**~535–620 MiB** cho cả 4 container).
  * Cấu hình đơn giản (1 file YAML + 1 file `.env`), xem log và debug trực tiếp bằng `docker compose logs`.
* **Nhược điểm:**
  * **Giới hạn khả năng Self-healing:** Policy `restart: unless-stopped` chỉ khởi động lại container khi tiến trình bên trong tự thoát; nếu container bị dừng từ bên ngoài (`docker stop`/`kill`), Docker gắn cờ dừng thủ công và không tự bật lại.
  * **Thiếu cơ chế mở rộng ngang (Horizontal Scaling) & Rolling Update chuẩn hóa:** Khó chia tải tự động nhiều worker trên nhiều máy chủ vật lý mà không gây gián đoạn cục bộ.

### Phương án 2: Dùng thuần túy Kubernetes (`k3d` / Managed K8s) cho mọi môi trường
* **Mô tả:** Triển khai toàn bộ các thành phần (kể cả Kafka/Zookeeper hoặc các worker) dưới dạng các tài nguyên Kubernetes (`Deployment`, `StatefulSet`, `ConfigMap`, `Secret`, `Service`).
* **Ưu điểm:**
  * Quản lý vòng đời ứng dụng chặt chẽ qua `ReplicaSet` và hệ thống 3 lớp probe (`startupProbe`, `livenessProbe`, `readinessProbe` giám sát file `/tmp/worker_heartbeat`).
  * Tự động tạo Pod thay thế ngay khi Pod bị xóa hoặc gặp lỗi, tách biệt hoàn toàn cấu hình (`ConfigMap`), thông tin nhạy cảm (`Secret`) và giới hạn tài nguyên (`requests`/`limits`).
* **Nhược điểm:**
  * **Chi phí tài nguyên cao trên máy cá nhân:** Riêng control plane của `k3d` (`k3d-lab-b-server-0` + `serverlb`) đã tiêu tốn **~580.5 MiB RAM** (gấp ~38 lần mức RAM `15 MiB` của bản thân Pod worker).
  * Thời gian khởi tạo lâu hơn 3–4 lần (**~19–25 giây** cho việc bật cụm, nối mạng `kafka-lab-net` và `k3d image import`), tăng độ phức tạp khi gỡ lỗi đa tầng (`kubectl describe`, `logs`, DNS/Network).

### Phương án 3: Kiến trúc kết hợp theo giai đoạn (Hybrid / Phased Strategy) — *Được chọn*
* **Mô tả:**
  * **Giai đoạn Local Development & Integration Testing:** Sử dụng **Docker Compose** (`infra/docker/compose.yaml`) để chạy trọn bộ pipeline trên máy.
  * **Giai đoạn Staging & Production (đặc biệt cho tầng Stateless Worker):** Sử dụng **Kubernetes** (`infra/k8s/`) để vận hành các consumer worker phi trạng thái, kết nối tới cụm Kafka độc lập.

---

## 4. Quyết định kiến trúc (Decision Outcome)

Chọn **Phương án 3 (Kiến trúc kết hợp theo giai đoạn)**: duy trì **Docker Compose** làm môi trường chuẩn cho phát triển cục bộ (Local Dev) và triển khai **Kubernetes** cho các dịch vụ **Stateless Worker** ở môi trường Staging/Production.

### Cơ sở lý giải từ số liệu thực nghiệm
1. **Tối ưu hiệu năng máy cục bộ:** Trên WSL2, Docker Compose tiết kiệm được gần **580 MiB RAM** chi phí control plane của K8s và rút ngắn thời gian dựng môi trường xuống còn **5–6 giây**, giúp kiểm thử nhanh logic xử lý sự kiện.
2. **Đảm bảo độ tin cậy ở Production:** Ở môi trường vận hành thực tế, Kubernetes giải quyết triệt để bài toán *Service Restart* thông qua `ReplicaSet` (tự động cấp phát Pod mới ngay lập tức khi Pod cũ bị xóa, không bị giới hạn bởi cờ dừng thủ công như Docker Engine) và giám sát sức khỏe event-loop thực chất thông qua heartbeat file thay vì chỉ kiểm tra tiến trình tồn tại.
3. **Tách biệt rõ ràng tầng Stateless và Stateful:** Việc thiết kế `worker.py` không phụ thuộc ổ đĩa cục bộ (kiểm chứng qua `emptyDir: {}` trong Lab B) cho phép Pod worker dễ dàng di chuyển giữa các node trên K8s mà không cần gắn ổ cứng cố định (`hostPath`).

---

## 5. Hệ quả & Đánh đổi (Consequences)

### Tích cực (Positive)
* **Tách cấu hình và bảo mật:** Toàn bộ thông số kết nối (`KAFKA_BROKER`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `RAW_DATA_PATH`) và khóa bí mật (`Secret`) được chuẩn hóa tên biến thống nhất giữa Compose và K8s, tránh hardcode trong mã nguồn.
* **Kiểm soát chặt chẽ Data Correctness:** Sự kết hợp giữa `enable_auto_commit=False`, `os.fsync()` trước `consumer.commit()` và handler bắt tín hiệu `SIGTERM` (giúp Pod tắt êm trong **~0.9 giây**) đảm bảo Pod mới nối tiếp chính xác `offset` cuối cùng của Pod cũ với tỷ lệ trùng lặp bằng `0` trong các kịch bản tắt có kiểm soát.

### Tiêu cực & Rủi ro cần kiểm soát (Negative & Mitigations)
* **Chi phí duy trì song song 2 bộ cấu hình:** Phải đồng bộ thay đổi biến môi trường giữa `infra/docker/compose.yaml` và `infra/k8s/configmap.yaml`. *Biện pháp:* Sử dụng chung một `Dockerfile` và quy ước đặt tên biến môi trường thống nhất cho cả hai môi trường.
* **Tính tạm thời của `emptyDir: {}` trên Kubernetes:** Khi Pod bị hủy và tái tạo trên K8s, dữ liệu lưu cục bộ trong `emptyDir` của Pod cũ sẽ bị xóa theo vòng đời Pod. *Biện pháp:* Ở môi trường Production, worker bắt buộc phải ghi dữ liệu ra kho lưu trữ bền vững bên ngoài (Object Storage như Data Warehouse) thay vì lưu file cục bộ trong container.
* **Rủi ro Replay khi sự cố cứng (Hard Crash / OOMKilled):** Nếu Pod bị tiêu diệt đột ngột (`SIGKILL`) ngay sau khi ghi đĩa nhưng trước khi gửi `OffsetCommit` về Kafka, lô tin nhắn cuối sẽ được phát lại (*at-least-once*). *Biện pháp:* Duy trì bước khử trùng lặp (Deduplication) theo khóa chính `event_id` ở tầng Staging/Silver của Data Warehouse để đảm bảo tính lũy đẳng (**Idempotency**) tuyệt đối.

---

## 6. Tiêu chí kích hoạt mở rộng sang Kubernetes (Scaling Triggers)

Hệ thống sẽ chuyển dịch toàn bộ tầng Worker sang vận hành chính thức trên Kubernetes (và tăng `replicas > 1`) khi xuất hiện một trong các điều kiện sau:

1. **Tăng số lượng Partition của Kafka Topic:** Topic `events` được chia thành $N \ge 2$ partitions để tăng thông lượng xử lý song song (trong Lab B hiện tại, topic chỉ có `1` partition nên cấu hình chuẩn là `replicas: 1` để tránh tạo Pod dự phòng nhàn rỗi).
2. **Yêu cầu High Availability đa node (Multi-node HA):** Hệ thống cần khả năng tự động chuyển dịch Pod sang máy chủ vật lý khác khi một node gặp sự cố phần cứng.
3. **Yêu cầu quản trị tài nguyên & triển khai tự động (CI/CD):** Cần áp đặt hạn mức `CPU/Memory requests & limits` nghiêm ngặt cho từng dịch vụ và thực hiện cập nhật phiên bản tự động qua pipeline CI/CD.