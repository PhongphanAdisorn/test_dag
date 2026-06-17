# Project Overview
โปรเจกต์นี้เป็น Data Pipeline (DAG) บน Apache Airflow ที่ใช้ในการดึงข้อมูลพื้นที่น้ำท่วมรายวัน (Flood 1Day) จาก API ของ `disaster-vallaris.gistda.or.th` เข้ามาบันทึกในฐานข้อมูล PostgreSQL (PostGIS) โดยอัตโนมัติ

มีการจัดการ Error Handling ผ่าน XCom หากการดึงข้อมูลผิดพลาด หรือบันทึกลง Database ไม่สำเร็จ ระบบจะทำการ Trigger DAG อีกตัวที่ชื่อ `trigger_traget_line_notify` เพื่อส่งการแจ้งเตือนไปที่ระบบ Line Notify

---

# Migration Notes (Airflow 2 to 3.2.2)
โปรเจกต์นี้ได้รับการอัปเกรดให้เข้ากับโครงสร้างของ **Apache Airflow 3.2.2** เรียบร้อยแล้ว ซึ่งมีสิ่งที่เปลี่ยนแปลงสำคัญ (Breaking Changes) ดังนี้:

- **Provider Packages:** Operator ที่เคยอยู่ในแกนกลางทั้งหมดได้ย้ายเข้าสู่ Provider Packages แล้ว เช่น `PythonOperator` และ `TriggerDagRunOperator` ถูกเรียกใช้จาก `airflow.providers.standard.operators` แทน `airflow.operators`
- **EmptyOperator:** ได้ยกเลิกการใช้ `DummyOperator` แล้วให้เปลี่ยนมาใช้ `EmptyOperator`
- **Exceptions:** โมดูล `AirflowFailException` และ `AirflowSkipException` ถูกย้ายไปที่ `airflow.sdk.exceptions`
- **PostgreSQL Hook:** การใช้งานได้เปลี่ยนจาก `airflow.hooks.postgres_hook` เป็น `airflow.providers.postgres.hooks.postgres`
- **DAG Context Manager:** เปลี่ยนการเขียน DAG จากแบบระบุตัวแปร `dag=dag` เป็นการครอบ Context ด้วย `with DAG(...) as dag:`
- **DAG Arguments:** เปลี่ยนแปลงจาก `schedule_interval` เป็น `schedule` รวมทั้งลบ `provide_context=True` ใน Operator ออกเพราะไม่มีความจำเป็นแล้ว

---

# Setup & Installation
สำหรับการรันใน Local หรือบนเซิร์ฟเวอร์ โปรดตรวจสอบว่าสภาพแวดล้อมมีส่วนประกอบดังนี้:

1. ติดตั้ง Airflow 3.2.2
   ```bash
   pip install apache-airflow==3.2.2
   ```

2. ติดตั้ง Providers ที่ต้องใช้
   ```bash
   pip install apache-airflow-providers-standard
   pip install apache-airflow-providers-postgres
   ```

3. ติดตั้ง Library พื้นฐานของโปรเจกต์
   ```bash
   pip install requests pendulum pytz
   ```

4. ตั้งค่า Connection ใน Airflow
   - ในหน้า Airflow UI ไปที่ `Admin` -> `Connections`
   - เพิ่ม Connection `farmai_conn` เป็นประเภท PostgreSQL และระบุรายละเอียดฐานข้อมูลเป้าหมายที่ต้องการให้บันทึก
   - ในหน้า Airflow UI ไปที่ `Admin` -> `Variables`
   - เพิ่ม Variable คีย์ `disaster_api_token` และใส่ค่า Token สำหรับเชื่อมต่อ API

---

# วิธีการรันและการทดสอบระบบเบื้องต้น
1. คัดลอกไฟล์ `jules_fool.py` ไปใส่ที่โฟลเดอร์ DAGs (โดยทั่วไปคือ `~/airflow/dags`)
2. ทดสอบว่า DAG โหลดได้สำเร็จโดยรันคำสั่งเช็คโครงสร้างไฟล์ผ่าน Terminal
   ```bash
   # หากไม่มี Output ที่แสดง Error ถือว่าโหลดสำเร็จ
   python -c "from airflow.models import DagBag; bag = DagBag(); print(bag.import_errors)"
   ```
3. รันเพื่อทดสอบ Task ทีละอันใน Local
   ```bash
   airflow tasks test Flood_1day call_disaster_api 2024-01-01
   ```
4. เปิด Airflow UI
   - หา DAG ชื่อ `Flood_1day` และเปิด Switch (Unpause DAG)
   - กดปุ่ม `Trigger DAG` มุมขวาบนเพื่อสั่งให้ทำงานทันที
   - เข้าไปที่ `Graph` หรือ `Grid` เพื่อสังเกตการณ์ทำงานของแต่ละ Task
