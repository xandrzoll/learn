## Задание

Что нужно сделать

Основываясь на описании владельца из домашнего задания к уроку 2:

Выделите домены нашей системы, их будет четыре при текущем уровне декомпозиции.
Выделите Core Domain (смысловое ядро).
Выберите и опишите тип связи между доменами из тех, что были на уроке. Объясните логику выбора.
Распишите агрегат для кухонного гарнитура и его составляющие (укажите примеры объектов значений, сущностей и бизнес-логики).

Будет не так просто, так как у вас нет возможности поговорить с доменными экспертами, но вспомните, что мы на уроке уже выделяли домены, но там не хватало одного — Core Domain. 


## Решение


<style>
.container {
    display: flex;

}

.rectangle {
    width: 150px;
    height: 50px;
    background-color: MediumSeaGreen;
    color: white;
    text-align: center;
    line-height: 50px;
    border: 1px solid black;
    font-size: 16px;
}

.arrow {
    width: 50px;
    height: 50px;
    background-color: transparent;
    position: relative;
    text-align: center;
    line-height: 40px;
    font-size: 34px;
}

</style>



<div class="container">
    <div class="rectangle" style="border: 3px dashed tomato">Мебель</div>
    <div class="arrow">&#x2192;</div>
    <div class="rectangle">Заказ</div>
    <div class="rectangle">Склад</div>
    <div class="rectangle">Логистика</div>
</div>