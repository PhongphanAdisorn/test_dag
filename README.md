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
สำหรับการรันบน Local หรือบนเซิร์ฟเวอร์ โปรดทำตามขั้นตอนต่อไปนี้เพื่อสร้าง Environment และติดตั้ง Dependencies อย่างถูกต้อง:

1. **สร้างและเปิดใช้งาน Virtual Environment (venv)** เพื่อป้องกัน Dependency Conflict
   ```bash
   # สร้าง venv ชื่อ 'airflow_env'
   python -m venv airflow_env

   # เปิดใช้งาน venv สำหรับ Linux/macOS
   source airflow_env/bin/activate

   # หรือเปิดใช้งาน venv สำหรับ Windows
   airflow_env\Scripts\activate
   ```

2. **ติดตั้ง Packages ทั้งหมดจาก `requirements.txt`**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

3. **ตั้งค่า Connection และ Variables ใน Airflow**
   - ในหน้า Airflow UI ไปที่ `Admin` -> `Connections`
   - เพิ่ม Connection `farmai_conn` เป็นประเภท PostgreSQL และระบุรายละเอียดฐานข้อมูลเป้าหมายที่ต้องการให้บันทึก
   - ในหน้า Airflow UI ไปที่ `Admin` -> `Variables`
   - เพิ่ม Variable คีย์ `disaster_api_token` และใส่ค่า Token สำหรับเชื่อมต่อ API (แทนที่การใส่ไว้ในโค้ดโดยตรงเพื่อความปลอดภัย)

---

# วิธีการรันและการทดสอบระบบเบื้องต้น (DAGs Testing)
การทดสอบจะช่วยให้มั่นใจได้ว่าโค้ดได้รับการ Refactor อย่างถูกต้องในสภาพแวดล้อม Airflow 3.2.2

1. **ตรวจสอบ Syntax Error (DAG Parsing)**
   คัดลอกไฟล์ `jules_fool.py` ไปที่โฟลเดอร์ DAGs ของคุณ (ค่าเริ่มต้นคือ `~/airflow/dags`)
   จากนั้นรันคำสั่งด้านล่างใน Terminal เพื่อเช็คว่า Airflow อ่านไฟล์และตีความสำเร็จโดยไม่มี Error
   ```bash
   python -c "from airflow.models import DagBag; bag = DagBag(dag_folder='jules_fool.py', include_examples=False); print(bag.import_errors)"
   # หากไม่มี Error จะแสดงผลลัพธ์ว่า {}
   ```

2. **ทดสอบรัน Task แบบ Standalone**
   สามารถทดสอบรัน Task ย่อยทีละตัวโดยไม่ต้องรันทั้ง Pipeline (เช่น ทดสอบ `call_disaster_api`)
   ```bash
   airflow tasks test Flood_1day call_disaster_api 2024-01-01
   ```

3. **ทดสอบรันทั้ง DAGs ผ่าน Command Line**
   สามารถรัน Pipeline ตั้งแต่ต้นจนจบในโหมดทดสอบ
   ```bash
   airflow dags test Flood_1day 2024-01-01
   ```

4. **การรันในโหมด Production ผ่าน Airflow UI**
   - เปิด Airflow UI หา DAG ชื่อ `Flood_1day` และเปิด Switch (Unpause DAG)
   - กดปุ่ม `Trigger DAG` (Play button) สั่งให้ทำงานทันที
   - เข้าไปที่ Tab `Graph` หรือ `Grid` เพื่อสังเกตการณ์ทำงานของแต่ละ Task
