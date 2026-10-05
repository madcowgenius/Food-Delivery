# 🍔 Distributed Food Delivery Platform

A microservices-based food delivery system built with **Ballerina**, **Apache Kafka**, **MongoDB**, and **Docker**.

## 📐 Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Customer   │     │  Restaurant  │     │    Admin     │
│   Service    │     │   Service    │     │   Service    │
│   :8082      │     │   :8083      │     │   :8087      │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       ▼                    ▼                    ▼
┌──────────────────────────────────────────────────────────┐
│                    MongoDB (food_delivery)                │
│  Collections: customers, restaurants, menu_items,        │
│  orders, payments, deliveries, drivers, notifications    │
└──────────────────────────────────────────────────────────┘
       ▲                    ▲                    ▲
       │                    │                    │
┌──────┴───────┐     ┌──────┴───────┐     ┌──────┴───────┐
│    Order     │     │   Payment    │     │   Delivery   │
│   Service    │◄───►│   Service    │◄───►│   Service    │
│   :8081      │     │   :8084      │     │   :8085      │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       ▼                    ▼                    ▼
┌──────────────────────────────────────────────────────────┐
│                    Apache Kafka                           │
│  Topics: orders.created, orders.status,                  │
│  payments.completed, delivery.assigned,                  │
│  delivery.completed, payments.refunded                   │
└──────────────────────────────────────────────────────────┘
       │                                         │
       ▼                                         ▼
┌──────────────┐                          ┌──────────────┐
│ Notification │                          │    Admin     │
│   Service    │                          │   Service    │
│   :8086      │                          │   :8087      │
└──────────────┘                          └──────────────┘
```

## 🔄 Event-Driven Flow (Kafka Topics)

```
Customer places order
       │
       ▼
[orders.created] ──► Payment Service (auto-processes payment)
       │                    │
       │                    ▼
       │            [payments.completed] ──► Delivery Service (auto-assigns driver)
       │                    │                       │
       │                    │                       ▼
       │                    │              [delivery.assigned] ──► Order Service (updates status)
       │                    │                       │
       │                    │                       ▼
       │                    │              [delivery.completed] ──► Order Service (marks DELIVERED)
       │                    │
       ▼                    ▼
Notification Service (listens to ALL topics, sends alerts)
Admin Service (logs ALL events for reporting)
```

## 📊 Order State Machine

```
CREATED ──► CONFIRMED ──► PREPARING ──► READY ──► OUT_FOR_DELIVERY ──► DELIVERED
   │            │              │           │
   └────────────┴──────────────┴───────────┴──────► CANCELLED
```

## 🛠️ Tech Stack

| Component      | Technology           |
|----------------|----------------------|
| Backend        | Ballerina 2201.13.5  |
| Messaging      | Apache Kafka 3.9.0   |
| Database       | MongoDB 7            |
| Containerisation | Docker + Docker Compose |
| Monitoring     | Kafka UI             |

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose installed
- (Optional) Ballerina 2201.13.5 for local development

### Run with Docker Compose
```bash
docker compose up --build
```

### Service Endpoints

| Service          | URL                          |
|------------------|------------------------------|
| Customer Service | http://localhost:8082/customers |
| Restaurant Service | http://localhost:8083/restaurants |
| Order Service    | http://localhost:8081/orders   |
| Payment Service  | http://localhost:8084/payments |
| Delivery Service | http://localhost:8085/deliveries |
| Notification Service | http://localhost:8086/notifications |
| Admin Service    | http://localhost:8087/admin    |
| Kafka UI         | http://localhost:8080          |

## 📡 API Endpoints

### Customer Service (:8082)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST   | /customers | Create customer |
| GET    | /customers | List all customers |
| GET    | /customers/{id} | Get customer by ID |
| PUT    | /customers/{id} | Update customer |
| DELETE | /customers/{id} | Delete customer |
| POST   | /customers/{id}/orders | Add order to history |
| GET    | /customers/{id}/orders | Get order history |

### Restaurant Service (:8083)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST   | /restaurants | Create restaurant |
| GET    | /restaurants | List all restaurants |
| GET    | /restaurants/{id} | Get restaurant |
| PUT    | /restaurants/{id} | Update restaurant |
| PUT    | /restaurants/{id}/status | Toggle open/close |
| POST   | /restaurants/{id}/menu | Add menu item |
| GET    | /restaurants/{id}/menu | Get menu |
| PUT    | /restaurants/{id}/menu/{itemId} | Update menu item |
| DELETE | /restaurants/{id}/menu/{itemId} | Delete menu item |
| PUT    | /restaurants/menu/{itemId}/decrement | Decrement stock |

### Order Service (:8081)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST   | /orders | Create order |
| GET    | /orders | List all orders |
| GET    | /orders/{id} | Get order |
| GET    | /orders/customer/{customerId} | Get orders by customer |
| PUT    | /orders/{id}/status | Update status (state machine) |
| PUT    | /orders/{id}/cancel | Cancel order |

### Payment Service (:8084)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST   | /payments | Process payment |
| GET    | /payments | List all payments |
| GET    | /payments/{id} | Get payment |
| GET    | /payments/order/{orderId} | Get payment by order |
| PUT    | /payments/{id}/refund | Refund payment |

### Delivery Service (:8085)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST   | /deliveries | Create delivery |
| GET    | /deliveries | List all deliveries |
| GET    | /deliveries/{id} | Get delivery |
| GET    | /deliveries/order/{orderId} | Get delivery by order |
| PUT    | /deliveries/{id}/status | Update delivery status |
| POST   | /deliveries/drivers | Register driver |
| GET    | /deliveries/drivers | List all drivers |
| GET    | /deliveries/drivers/available | List available drivers |
| PUT    | /deliveries/drivers/{id}/location | Update driver location |

### Notification Service (:8086)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST   | /notifications | Send notification |
| GET    | /notifications | List all notifications |
| GET    | /notifications/{id} | Get notification |
| GET    | /notifications/recipient/{id} | Get by recipient |

### Admin Service (:8087)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET    | /admin/health | Health check |
| GET    | /admin/dashboard | Dashboard overview |
| GET    | /admin/reports/orders | Order statistics |
| GET    | /admin/reports/deliveries | Delivery performance |
| GET    | /admin/reports/payments | Payment statistics |
| GET    | /admin/reports/restaurants | Restaurant statistics |
| GET    | /admin/events | Audit event log |

## 🧪 Testing the Flow

```bash
# 1. Register a driver
curl -X POST http://localhost:8085/deliveries/drivers \
  -H "Content-Type: application/json" \
  -d '{"name": "John Driver", "phone": "+264811234567", "vehicleType": "motorcycle"}'

# 2. Create a customer
curl -X POST http://localhost:8082/customers \
  -H "Content-Type: application/json" \
  -d '{"name": "Jane Doe", "email": "jane@email.com", "phone": "+264812345678", "addresses": ["123 Main St, Windhoek"]}'

# 3. Create a restaurant
curl -X POST http://localhost:8083/restaurants \
  -H "Content-Type: application/json" \
  -d '{"name": "Joe Brews", "address": "456 Independence Ave", "phone": "+264813456789", "openingHours": "08:00", "closingHours": "22:00"}'

# 4. Place an order (triggers the entire event chain automatically!)
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "<CUSTOMER_ID>", "restaurantId": "<RESTAURANT_ID>", "items": [{"menuItemId": "item1", "name": "Burger", "quantity": 2, "price": 45.00}], "total": 90.00}'

# 5. Check order status (should auto-progress via Kafka events)
curl http://localhost:8081/orders/<ORDER_ID>

# 6. Check admin dashboard
curl http://localhost:8087/admin/dashboard

# 7. Check notifications generated
curl http://localhost:8086/notifications
```

## 📁 Project Structure
```
Food-Delivery/
├── customer-service/
│   ├── main.bal
│   ├── Ballerina.toml
│   └── Dockerfile
├── restaurant-service/
│   ├── main.bal
│   ├── Ballerina.toml
│   └── Dockerfile
├── order-service/
│   ├── main.bal
│   ├── Ballerina.toml
│   └── Dockerfile
├── payment-service/
│   ├── main.bal
│   ├── Ballerina.toml
│   └── Dockerfile
├── delivery-service/
│   ├── main.bal
│   ├── Ballerina.toml
│   └── Dockerfile
├── notification-service/
│   ├── main.bal
│   ├── Ballerina.toml
│   └── Dockerfile
├── admin-service/
│   ├── main.bal
│   ├── Ballerina.toml
│   └── Dockerfile
├── docker-compose.yml
└── README.md
```

## 👥 Group Members
- Member 1 - Student ID
- Member 2 - Student ID
- Member 3 - Student ID
- Member 4 - Student ID

## 📜 License
This project is an academic assignment for DSA612S — Distributed Systems and Applications.
