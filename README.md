# s3-log-rs (WIP)

### **WAL in Rust Using S3**

A **distributed, reliable, and highly available write-ahead log (WAL)** built on top of Amazon S3. This implementation leverages S3's durability and scalability to provide a fault-tolerant log for distributed systems.

The project draws inspiration from [this insightful blog post](https://avi.im/blag/2024/s3-log/) by Avi. 

---

### **Key Features**
- **Distributed**: Ideal for use in distributed systems where durability and fault-tolerance are critical.
- **Reliable**: Ensures log integrity through atomic writes and efficient reads.
- **Highly Available**: S3's inherent high availability makes it an excellent storage layer for WAL.

---

### **How It Works**
This WAL implementation stores log entries as individual objects in an S3 bucket, using S3's capabilities to provide:
- **Durability**: Data stored in S3 is replicated across multiple availability zones.
- **Scalability**: Handles large-scale write and read operations seamlessly.
- **Fault Tolerance**: Survives disk or node failures by design.

---

### **Use Cases**
- Distributed databases and consensus systems.
- Event-sourcing applications.
- Any application requiring a persistent, highly available append-only log.

---

### **Getting Started**
1. Clone the repository and follow the setup instructions.
2. Ensure an S3-compatible storage system (e.g., AWS S3, MinIO) is accessible.
3. Integrate the WAL into your distributed application using Rust.

---

This project is designed to make distributed system design simpler by providing a foundational building block for consistent state management, using S3 as a robust backing store.
