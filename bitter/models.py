import time

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import BigInteger, Integer, Text, Boolean
from sqlalchemy import Column, Index

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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

def make_session(url):
    engine = create_engine(url)#, echo=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session
    


def test(db='sqlite:///users.db'):

    from sqlalchemy import exists
    session = make_session(db)

    our_user = session.query(User).first() 

    print(our_user.name)
    print(session.query(User).count())
    fake_user = User(name="Fake user")
    session.add(fake_user)
    session.commit()
    print(session.query(User).count())
    print(session.query(exists().where(User.name == "Fake user")).scalar())
    fake_committed = session.query(User).filter_by(name="Fake user").first()
    print(fake_committed.id)
    print(fake_committed.name)
    session.delete(fake_committed)
    session.commit()
    print(session.query(User).count())
    print(list(session.execute('SELECT 1 from users where id=\'%s\'' % 1548)))
