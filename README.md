# Микросервисная система обработки заказов

проект, демонстрирующий реализацию микросервисной архитектуры для электронной коммерции на FastAPI.

---

- [Архитектура](#архитектура)
- [Технологический стек](#-технологический-стек)
- [API Endpoints](#-api-endpoints)

## Архитектура

Система построена по принципам микросервисной архитектуры с использованием брокера сообщений для межсервисного взаимодействия:

```mermaid
API --> Order[Order Service]
Order --> RMQ[RabbitMQ]
RMQ --> Payment[Payment Service]
RMQ --> Shipping[Shipping Service]
RMQ --> Notification[Notification Service]

Order --> Order DB
Payment --> Logs
Shipping --> Shipping DB
Notification --> Logs   
```

## Технологический стек

### Бэкенд
- FastAPI - современный асинхронный веб-фреймворк
- SQLAlchemy 2.0 - асинхронный ORM
- AsyncPG - асинхронный драйвер PostgreSQL
- AIO-Pika - асинхронный клиент RabbitMQ
- Pydantic - валидация и сериализация данных

### Инфраструктура
- PostgreSQL 17 - реляционная база данных
- RabbitMQ - брокер сообщений
- Docker - контейнеризация приложений
- Docker Compose - оркестрация контейнеров

### Вспомогательные инструменты
- PgAdmin - веб-интерфейс для управления БД
- Uvicorn - ASGI-сервер для FastAPI

## API Endpoints
### Order Service
- POST /orders/ - Создание заказа

- GET /orders/ - Список заказов

- GET /orders/{id} - Информация о заказе

- PATCH /orders/{id}/status - Обновление статуса

- DELETE /orders/{id} - Удаление заказа