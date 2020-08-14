import asyncio
from datetime import datetime, timedelta
from operator import attrgetter
from typing import List, Optional, AsyncIterator, Iterator

from aioredis import create_redis_pool
from pydantic import BaseModel, Field
import pytest
from testcontainers.redis import RedisContainer

from radish import Database, Interface, RadishError, RadishKeyError


pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="session")
def redis_address() -> Iterator[str]:
    with RedisContainer("redis:6-alpine") as redis:
        yield f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}"


@pytest.fixture(autouse=True)
async def redis(redis_address: str):
    redis = await create_redis_pool(address=redis_address)
    try:
        yield redis
    finally:
        await redis.flushall()
        redis.close()
        await redis.wait_closed()


class User(BaseModel):
    id: int
    name: str


class ToDo(BaseModel):
    id: int
    text: str
    created: datetime = Field(default_factory=datetime.now)
    due: Optional[datetime] = None
    author: User


class Radish(Interface):
    users = Database(1, User, key="id")
    todos = Database(2, ToDo, key="id")


@pytest.fixture()
async def radish(redis_address: str) -> AsyncIterator[Radish]:
    async with Radish(address=redis_address) as interface:
        yield interface


@pytest.fixture()
async def user(radish: Radish):
    user = User(id=1, name="bob")
    await radish.users.save(user)
    return user


class TestInterface:
    @staticmethod
    async def test_it_raises_on_operation_outside_of_contextmanager(redis_address: str):
        with pytest.raises(RadishError):
            radish = Radish(address=redis_address)
            await radish.users.save(User(id=99, name="ozymandias"))

    @staticmethod
    async def test_it_raises_on_reconnect(redis_address: str):
        radish = Radish(address=redis_address)
        with pytest.raises(RadishError):
            async with radish:
                async with radish:
                    pass

    @staticmethod
    async def test_it_raises_on_reserved_class_attr():
        with pytest.raises(RadishError):
            class _Radish(Interface):
                users = Database(1, User, key="id")
                todos = Database(2, ToDo, key="id")
                _meta = "foo"

class TestSave:
    @staticmethod
    @pytest.mark.parametrize("allow_update", [True, False])
    async def test_it_creates_non_existing_record(radish: Radish, allow_update: bool):
        user = User(id=1, name="bob")
        await radish.users.save(user, allow_update=allow_update)
        assert await radish.users.get(user.id) == user

    @staticmethod
    async def test_it_updates_existing_record_default(radish: Radish, user: User):
        user.name = "fred"
        await radish.users.save(user)
        assert (await radish.users.get(user.id)).name == "fred"

    @staticmethod
    async def test_it_raises_on_disallowed_update(radish: Radish, user: User):
        user.name = "fred"
        with pytest.raises(RadishError):
            await radish.users.save(user, allow_update=False)

    @staticmethod
    async def test_it_fails_on_bad_input_model(radish: Radish, user: User):
        todo = ToDo(id=1, author=user, text="scrub the deck")
        with pytest.raises(RadishError):
            await radish.users.save(todo)  # type: ignore

    @staticmethod
    async def test_it_fails_on_subclass_input_model(radish: Radish, user: User):
        class SubUser(User):
            pass
        sub_user = SubUser(id=2, name="pete")
        with pytest.raises(RadishError):
            await radish.users.save(sub_user)

    @staticmethod
    async def test_it_can_set_expiry_when_creating_record(radish: Radish):
        user = User(id=1, name="bob")
        await radish.users.save(user, expire=0.5)
        assert await radish.users.get(user.id, None)
        await asyncio.sleep(0.5)
        assert not await radish.users.get(user.id, None)


class TestRetrieve:
    @staticmethod
    async def test_it_can_retrieve_record_by_id(radish: Radish, user: User):
        result = await radish.users.get(user.id)
        assert result == user

    @staticmethod
    async def test_it_can_retrieve_record_by_instance(radish: Radish, user: User):
        result = await radish.users.get(user)
        assert result == user

    @staticmethod
    async def test_it_returns_up_to_redis_record_when_retrieving_by_instance(
        radish: Radish, user: User
    ):
        user.name = "fred"
        result = await radish.users.get(user)
        assert result.name == "bob"

    @staticmethod
    async def test_it_retrieves_existing_record_when_default_is_passed(
        radish: Radish, user: User
    ):
        result = await radish.users.get(user.id, default="default")
        assert result == user

    @staticmethod
    async def test_it_fails_when_retrieving_record_which_does_not_exist(
        radish: Radish, user: User
    ):
        with pytest.raises(RadishKeyError):
            await radish.users.get(2)

    @staticmethod
    async def test_it_returns_default_when_retrieving_record_which_does_not_exist(
        radish: Radish, user: User
    ):
        result = await radish.users.get(2, default="default")
        assert result == "default"


class TestDelete:
    @staticmethod
    async def test_it_deletes_existing_record_by_id(radish: Radish, user: User):
        await radish.users.delete(user.id)
        assert not await radish.users.get(user.id, None)

    @staticmethod
    async def test_it_deletes_existing_record_by_instance(radish: Radish, user: User):
        await radish.users.delete(user)
        assert not await radish.users.get(user.id, None)

    @staticmethod
    async def test_it_raises_when_deleting_nonexistent_record(radish: Radish, user: User):
        with pytest.raises(RadishKeyError):
            await radish.users.delete(2)


class TestExpire:
    @staticmethod
    async def test_it_expires_existing_record_by_id(radish: Radish, user: User):
        await radish.users.expire(user.id, 0.1)
        await asyncio.sleep(0.2)
        assert not await radish.users.get(user.id, None)

    @staticmethod
    async def test_it_raises_when_expiring_nonexistent_record(radish: Radish, user: User):
        with pytest.raises(RadishKeyError):
            await radish.users.expire(2, 0.1)


class TestAIter:
    @staticmethod
    @pytest.fixture()
    async def users(radish: Radish) -> List[User]:
        users = [
            User(id=idx, name=name) for idx, name in enumerate(["fred", "bob", "harry"])
        ]
        await asyncio.gather(*[radish.users.save(user) for user in users])
        return users

    @staticmethod
    async def test_it_iterates_over_users(radish: Radish, users: List[User]):
        results = [user async for user in radish.users]
        assert sorted(results, key=attrgetter("id")) == users


class TestFilter:
    @staticmethod
    @pytest.fixture()
    async def users(radish: Radish) -> List[User]:
        users = [
            User(id=idx, name=name) for idx, name in enumerate(["fred", "bob", "harry"])
        ]
        await asyncio.gather(*[radish.users.save(user) for user in users])
        return users

    @staticmethod
    async def test_it_filters_on_single_string_attr(radish: Radish, users: List[User]):
        results = [user async for user in radish.users.filter(name="bob")]
        assert results == [users[1]]

    @staticmethod
    async def test_filter_raises_on_bad_kwargs(radish: Radish, user: List[User]):
        with pytest.raises(RadishError):
            _ = [user async for user in radish.users.filter(nom="bob")]

    @staticmethod
    @pytest.fixture()
    async def todos(radish: Radish, users: List[User]) -> List[ToDo]:
        todo_data = [
            (text, author)
            for text in ["take out trash", "mow the lawn"]
            for author in users
        ]
        todos = [
            ToDo(id=idx, text=text, author=author, due=datetime.now() + timedelta(days=idx))
            for idx, (text, author) in enumerate(todo_data)
        ]
        await asyncio.gather(*[radish.todos.save(todo) for todo in todos])
        return todos

    @staticmethod
    async def test_it_filters_on_model_instance_attr(
        radish: Radish, users: List[User], todos: List[ToDo]
    ):
        author = users[-1]
        results = [todo async for todo in radish.todos.filter(author=author)]
        assert len(results) == 2
        assert {result.author.name for result in results} == {"harry"}

    @staticmethod
    async def test_it_filters_on_multiple_attrs(
        radish: Radish, users: List[User], todos: List[ToDo]
    ):
        author = users[-1]
        results = [
            todo async for todo in radish.todos.filter(author=author, text="mow the lawn")
        ]
        assert len(results) == 1
        assert results[0].author == author
        assert results[0].text == "mow the lawn"
