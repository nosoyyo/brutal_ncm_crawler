import redis
import pymongo
import requests

# code -460: cheating
# code 400: nothing there
# code 404: nothing at all, even no error msg
# code 500: internal error
# code 502: tunneling socket could not be established


# init
r = redis.Redis(db=6)
host = 'http://49.235.161.254:18888'
client = pymongo.MongoClient('127.0.0.1')
db = client.get_database('nem')
artist_album_col = db.get_collection('artist_album')
album_col = db.get_collection('albums')
song_col = db.get_collection('songs')


def get_new_proxy():
    '''
    :var pack: 63091 the free trial 500
    :var pack: 63089 the free daily 20
    '''
    #pack = 63091
    #pack = 63089
    #url = f'http://http.tiqu.alicdns.com/getip3?num=1&type=2&pack={pack}'
    url = 'http://http.tiqu.alicdns.com/getip3?num=1&type=2&time=1'
    resp = requests.get(url).json()
    ip = resp['data'][0]['ip']
    port = resp['data'][0]['port']
    return f'socks5://{ip}:{port}'



# get all the artist ids
with open('artists.csv', 'r') as f:
    artists = f.read()
    artists = list(set([i.split(',')[0] for i in artists.split('\n') if i]))


def grab_artist_album(todo):
    '''
    :param todo: <list> a list of artist id
    '''
    global proxy
    finished = 0
    p_used = 0
    print(f'{len(todo)} to go')

    for artist_id in todo:
        if not artist_album_col.find_one({'id': artist_id}):
            artist_album_url = f'{host}/artist/album?id={artist_id}&proxy={proxy}&offset=0&limit=100'
            resp = requests.get(artist_album_url).json()
            resp['id'] = artist_id
            if resp['code'] == 200:
                artist_album_col.insert_one(resp)
                finished += 1
                print(
                    f'{artist_id} saved. {finished} finished. {len(todo) - finished} remaining')
            elif resp['code'] == 502 or resp['code'] == -460:
                proxy = get_new_proxy()
                p_used += 1
                print(f'new proxy acquired. {p_used} used.')
                continue
            elif resp['code'] == 400 or resp['code'] == 404:
                continue
            elif resp['code'] == 500:
                print(f'artist: {artist_id} msg:{resp}')
                continue
            else:
                proxy = get_new_proxy()
                p_used += 1
                print(f'new proxy acquired. {p_used} used.')
                print(f'artist: {artist_id} msg:{resp}')
                continue
        else:
            print(f'{artist_id} already existed. skipping...')


def grab_album():
    finished = 0
    p_used = 0
    global proxy

    aid = str(int(r.spop('todo_set')))

    if not r.sismember('done', aid):
        artist = [i for i in artist_album_col.find({'id': f'{aid}'})][0]
        for album in artist['hotAlbums']:
            album_id = album['id']
            album_url = f'{host}/album?id={album_id}&proxy={proxy}'
            resp = requests.get(album_url).json()
            resp['id'] = album_id
            # normal condition as expected
            if resp['code'] == 200:
                album_col.insert_one(resp)
                finished += 1
                r.sadd('done', aid)
                print(f'{album_id} saved. {finished} album finished.')
            elif resp['code'] in [502, 503, -460]:
                # send the id back into todo set
                r.sadd('todo_set', aid)
                proxy = get_new_proxy()
                p_used += 1
                print(f'new proxy acquired. {p_used} used.')
                continue
            elif resp['code'] == 400 or resp['code'] == 404:
                continue
            else:
                # send the id back into todo set
                r.sadd('todo_set', aid)
                print(f'something wrong. {aid} put back')
                # tell daddy whats going on
                print(f'artist: {aid} msg:{resp}')
                continue
    else:
        print(f'artist {aid} already crawled. skipping...')


def main():
    while len(r.smembers('todo_set')):
        try:
            grab_album()
        except Exception as e:
            print(e)
