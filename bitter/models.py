import time
import json

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import BigInteger, Integer, Text, Boolean
from sqlalchemy.schema import ForeignKey
from sqlalchemy.pool import SingletonThreadPool
from sqlalchemy import Column, Index

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from functools import wraps

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    id = Column(BigInteger, primary_key=True, index=True, unique=True)
    contributors_enabled = Column(Boolean)
    created_at_stamp = Column(Text)
    default_profile = Column(Boolean)
    default_profile_image = Column(Boolean)
    description = Column(Text)
    entities = Column(Text)
    favourites_count = Column(Integer)
    followers_count = Column(Integer)
    following = Column(Boolean)
    friends_count = Column(Integer)
    geo_enabled = Column(Boolean)
    has_extended_profile = Column(Boolean)
    id_str = Column(Text)
    is_translation_enabled = Column(Boolean)
    is_translator = Column(Boolean)
    lang = Column(Text)
    listed_count = Column(Integer)
    location = Column(Text)
    name = Column(Text)
    notifications = Column(Boolean)
    profile_background_color = Column(Text)
    profile_background_image_url = Column(Text)
    profile_background_image_url_https = Column(Text)
    profile_background_tile = Column(Boolean)
    profile_banner_url = Column(Text)
    profile_image_url = Column(Text)
    profile_image_url_https = Column(Text)
    profile_link_color = Column(Text)
    profile_sidebar_border_color = Column(Text)
    profile_sidebar_fill_color = Column(Text)
    profile_text_color = Column(Text)
    profile_use_background_image = Column(Boolean)
    protected = Column(Boolean)
    screen_name = Column(Text)
    statuses_count = Column(Integer)
    time_zone = Column(Text)
    url = Column(Text)
    utc_offset = Column(Integer)
    verified = Column(Boolean)


    def as_dict(self):
        dcopy = self.__dict__.copy()
        for k,v in self.__dict__.items():
            if k[0] == '_':
                del dcopy[k]
        try:
            dcopy['entities'] = json.loads(dcopy['entities'])
        except Exception:
            print('Could not convert to dict')
            pass
        return dcopy

class Following(Base):
    __tablename__ = 'followers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    isfollowed = Column(Integer)
    follower = Column(Integer)
    created_at_stamp = Column(Text)

    follower_index = Index('isfollowed', 'follower')

class ExtractorEntry(Base):
    __tablename__ = 'extractor-cursor'

    id = Column(Integer, primary_key=True, default=lambda x: int(time.time()*1000))
    user = Column(BigInteger, index=True)
    cursor = Column(BigInteger, default=-1)
    pending = Column(Boolean, default=False)
    errors = Column(Text, default="")
    busy = Column(Boolean, default=False)


class Search(Base):
    __tablename__ = 'search_queries'

    id = Column(Integer, primary_key=True, index=True, unique=True)
    endpoint = Column(Text, comment="Endpoint URL")
    attrs = Column(Text, comment="Text version of the dictionary of parameters")
    count = Column(Integer)
    current_count = Column(Integer)
    current_id = Column(BigInteger, comment='Oldest ID retrieved (should match max_id when done)')
    since_id = Column(BigInteger)

class SearchResults(Base):
    __tablename__ = 'search_results'
    id = Column(Integer, primary_key=True, index=True, unique=True)
    search_id = Column(ForeignKey('search_queries.id'))
    resource_id = Column(Text)

def memoize(f):
    memo = {}
    @wraps(f)
    def helper(self, **kwargs):
        st = dict_to_str(kwargs)
        key = (self.__uriparts, st)
        if key not in memo:
            memo[key] = f(self, **kwargs)
        return memo[key]
    return helper

def make_session(url):
    if not isinstance(url, str):
        print(url)
        raise Exception("FUCK")
    engine = create_engine(url, poolclass=SingletonThreadPool)#, echo=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def dict_to_str(args):
    return json.dumps(args, sort_keys=True)
