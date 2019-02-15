### async-orm
Async orm for async web framework, you should use Django to manage your database(ddl etc).
```
pip install -U async-orm
```

### Usage
```
from django.db import models
from async_orm import BaseModel, transation, Q


class FooBar(BaseModel):
    """ foo bar """

    game = models.CharField(verbose_name='game', max_length=30, blank=False, null=False, db_index=True)
    title = models.CharField(verbose_name='title', max_length=100, blank=False, null=False)
    weight = models.IntegerField(verbose_name='weight', blank=False, null=False, default=0)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = 'test'
        verbose_name = "foo_bar"
        verbose_name_plural = "foo_bar"
        ordering = ['key', '-title']


async def test_foo_bar():
    game = 'game1'
    title = 'title2'
    # select
    await FooBar.select(where=Q(game=game, title=title) | Q(game='game2'), limit=2, offset=5)
    await FooBar.select(fields=['id', 'title'], order_by=['-title'])
    await FooBar.select(where=Q(weight__gt=0), limit=1)
    await FooBar.select(count=['game'], distinct=True, where=Q(id__gte=2))

    ret = await FooBar.select(where=Q(game='game2'), limit=1)
    if not ret:
        # insert
        ret = FooBar(game='game2', title='title22', weight=0)
        num = await ret.save()
        print(ret.id)
        print(num)
        # result = FooBar.create(game='game2', title='title22', weight=0)
    else:
        # update
        ret.title = 'new title' + str(datetime.datetime.now())
        num = await ret.save()
        print(ret.id)
        print(num)

        # delete
        # num = await ret.delete()
        # print(num)

    # transaction
    if ret is not None:
        async with transaction() as ts:
            await FooBar.create(game='game2', title='ttitle1', weight=0, ts=ts)
            await FooBar.create(game='game2', title='ttitle2', weight=0, ts=ts)
            await FooBar.create(game='game2', title='ttitle3', weight=0, ts=ts)
            await ts.conn.rollback()
            await FooBar.create(game='game2', title='ttitle4', weight=0, ts=ts)
            await ts.conn.commit()

```
