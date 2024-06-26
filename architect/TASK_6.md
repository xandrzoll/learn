### Выделенный микросервис (Core Domain)
Микросервис "Управление товарами" будет отвечать за продукты в системе. Он будет отвечать за хранение 
информации о продуктах, управление каталогом, их доступностью на складе и другие связанные с продуктами 
операции.

### Интеграции и их роли в бизнес-процессах
1. Интеграция с **Складской системой:**
   - Необходимо получать информацию о имеющихся товарах и их остатках.
   - Возможность бронировать остатки товаров при формировании заказа.
   - Изменять остаток после формирования заказа или при его отмене.
2. Интеграция с **Логистической системой**
   - Получать и передавать в Логистическую систему информацию о заказе и адресе доставки.
   - Отслеживание доставки

### ФТ и НФТ к интеграции с Микросервисом Управления Товарами
1. **Функциональные требования (ФТ):**
   - **Добавление товара в корзину:**
       - После выбора товара, он должен успешно добавиться в корзину пользователя.
   - **Отображение информации о продукте:**
       - Информация о продукте (название, описание, цена) должна корректно отображаться на сайте.
   - **Передача информации об адресе доставки**:
     - Пользователь может указать в форме свой адрес доставки, эта информация должна передаваться в Складскую систему.
   
2. **Нефункциональные требования (НФТ):**
   - **Скорость ответа:**
       - Система управления товарами должна предоставлять информацию о продуктах в течение 100 мс для 
    обеспечения быстрого отображения на сайте.
   - **Надежность:**
       - Интеграция должна быть доступна 99.9% времени для обеспечения непрерывной работы процессов заказа.
   - **Масштабируемость:**
       - Микросервис должен быть способен обрабатывать запросы о продуктах с ростом числа пользователей и объема данных.
   - **Отказоустойчивость:**
       - Система должна быть устойчива к сбоям, предотвращать потерю данных о продуктах и восстанавливаться автоматически.

