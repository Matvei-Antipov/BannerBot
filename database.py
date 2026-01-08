import aiosqlite
import json
import math

DB_NAME = 'bot_database.db'

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # 1. Юзеры
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                is_admin INTEGER DEFAULT 0,
                promoted_by TEXT DEFAULT NULL
            )
        ''')

        # 2. Команды
        await db.execute('''
            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                tag TEXT,
                rank INTEGER DEFAULT 0,
                roster TEXT,
                logo_base64 TEXT,
                games_ids TEXT,
                achievements TEXT
            )
        ''')

        # 3. Турниры
        await db.execute('''
            CREATE TABLE IF NOT EXISTS tournaments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT,
                season TEXT,
                year INTEGER DEFAULT 2024,
                has_qualifiers BOOLEAN,
                has_group_stage BOOLEAN,
                logo_base64 TEXT,
                prize_data TEXT,
                mvp_data TEXT,
                participants TEXT,
                winners TEXT,
                is_active BOOLEAN DEFAULT 1
            )
        ''')

        # 4. ИГРЫ
        await db.execute('''
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id INTEGER,
                game_date TEXT,
                game_format TEXT,
                map_name TEXT,
                team1_tag TEXT,
                team2_tag TEXT,
                score_t1 INTEGER,
                score_t2 INTEGER,
                total_rounds INTEGER,
                stats_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 5. ТРАНСФЕРЫ
        await db.execute('''
            CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name TEXT,
                old_team TEXT,
                new_team TEXT,
                date TEXT
            )
        ''')

        # 6. МЕТАДАННЫЕ ИГРОКОВ
        await db.execute('''
            CREATE TABLE IF NOT EXISTS player_metadata (
                nickname TEXT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                photo_file_id TEXT
            )
        ''')

        # Миграции
        try: await db.execute("ALTER TABLE tournaments ADD COLUMN year INTEGER DEFAULT 2024")
        except: pass
        try: await db.execute("ALTER TABLE tournaments ADD COLUMN season TEXT DEFAULT ''")
        except: pass
        try: await db.execute("ALTER TABLE tournaments ADD COLUMN participants TEXT DEFAULT '[]'")
        except: pass
        try: await db.execute("ALTER TABLE tournaments ADD COLUMN winners TEXT DEFAULT '{}'")
        except: pass
        try: await db.execute("ALTER TABLE users ADD COLUMN promoted_by TEXT DEFAULT NULL")
        except: pass
        try: await db.execute("ALTER TABLE games ADD COLUMN game_date TEXT")
        except: pass
        try: await db.execute("ALTER TABLE games ADD COLUMN game_format TEXT")
        except: pass

        await db.commit()

    await ensure_fft_team()

async def ensure_fft_team():
    if not await check_team_exists("Free Agents", "FFT"):
        await create_team("Free Agents", "FFT", "", "")

# =======================
#        ЮЗЕРЫ
# =======================

async def add_user(user_id, username):
    role = 2 if username == "matvei_dev" else 0
    sys_promo = "SYSTEM" if role == 2 else None
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT INTO users (user_id, username, is_admin, promoted_by)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                is_admin = CASE WHEN username = 'matvei_dev' THEN 2 ELSE is_admin END,
                promoted_by = CASE WHEN username = 'matvei_dev' AND promoted_by IS NULL THEN 'SYSTEM' ELSE promoted_by END
        ''', (user_id, username, role, sys_promo))
        await db.commit()

async def get_user_info(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM users WHERE user_id=?',(user_id,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

async def check_is_admin(uid):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute('SELECT is_admin FROM users WHERE user_id=?',(uid,)) as cur:
            res = await cur.fetchone()
            return (res[0] >= 1) if res else False

async def check_is_owner(uid):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute('SELECT is_admin FROM users WHERE user_id=?',(uid,)) as cur:
            res = await cur.fetchone()
            return (res[0] >= 2) if res else False

async def set_admin_role(target_username, promoter, role_level):
    target_clean = target_username.replace("@", "")
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('UPDATE users SET is_admin=?, promoted_by=? WHERE username=?', (role_level, promoter, target_clean))
        await db.commit()

async def remove_admin_role(user_db_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('UPDATE users SET is_admin=0, promoted_by=NULL WHERE user_id=?', (user_db_id,))
        await db.commit()

async def get_admins_paginated(page=0, limit=3):
    offset = page * limit
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT COUNT(*) FROM users WHERE is_admin > 0') as cur:
            total_count = (await cur.fetchone())[0]
        query = 'SELECT user_id, username, is_admin, promoted_by FROM users WHERE is_admin > 0 ORDER BY is_admin DESC, username ASC LIMIT ? OFFSET ?'
        async with db.execute(query, (limit, offset)) as cursor:
            admins = [dict(row) for row in await cursor.fetchall()]
    total_pages = math.ceil(total_count / limit)
    return admins, total_pages, total_count

async def get_user_by_db_id(id_val):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM users WHERE user_id=?',(id_val,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

# =======================
#       КОМАНДЫ
# =======================

async def check_team_exists(name, tag):
    async with aiosqlite.connect(DB_NAME) as db:
        sql = 'SELECT id FROM teams WHERE LOWER(name) = LOWER(?) OR LOWER(tag) = LOWER(?)'
        async with db.execute(sql, (name, tag)) as cursor:
            return True if await cursor.fetchone() else False

async def get_team_by_tag(tag):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        sql = 'SELECT * FROM teams WHERE LOWER(tag) = LOWER(?)'
        async with db.execute(sql, (tag,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

async def create_team(name, tag, roster, logo_base64):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT INTO teams (name, tag, rank, roster, logo_base64, games_ids, achievements)
            VALUES (?, ?, 0, ?, ?, "[]", "[]")
        ''', (name, tag, roster, logo_base64))
        await db.commit()

async def get_team_by_id(team_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM teams WHERE id = ?', (team_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

async def delete_team(team_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('DELETE FROM teams WHERE id = ?', (team_id,))
        await db.commit()

async def update_team_field(team_id, field, val):
    if field not in ['name', 'tag', 'roster', 'logo_base64']: return False
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(f'UPDATE teams SET {field}=? WHERE id=?', (val, team_id))
        await db.commit()
    return True

async def get_teams_paginated(page=0, limit=3, sort_by='tag'):
    offset = page * limit
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT COUNT(*) FROM teams') as cur:
            total_count = (await cur.fetchone())[0]
        if sort_by == 'name': order_sql = "ORDER BY LOWER(name) ASC"
        else: order_sql = "ORDER BY LOWER(tag) ASC"
        query = f'SELECT id, name, tag FROM teams {order_sql} LIMIT ? OFFSET ?'
        async with db.execute(query, (limit, offset)) as cursor:
            teams = [dict(row) for row in await cursor.fetchall()]
    total_pages = math.ceil(total_count / limit)
    return teams, total_pages, total_count

# --- ФУНКЦИИ ДЛЯ УПРАВЛЕНИЯ УЧАСТНИКАМИ ТУРНИРА ---
async def add_team_to_tournament(tournament_id: int, team_id: int):
    """Добавляет команду в список участников турнира"""
    async with aiosqlite.connect(DB_NAME) as db:
        # Получаем текущих участников
        async with db.execute('SELECT participants FROM tournaments WHERE id=?', (tournament_id,)) as cur:
            row = await cur.fetchone()
            if not row:
                return False

        try:
            participants = json.loads(row[0]) if row[0] else []
        except:
            participants = []

        # Проверяем, не добавлена ли уже команда
        if team_id in participants:
            return False

        # Добавляем команду
        participants.append(team_id)

        # Обновляем в БД
        await db.execute('UPDATE tournaments SET participants=? WHERE id=?',
                        (json.dumps(participants), tournament_id))
        await db.commit()
        return True

async def remove_team_from_tournament(tournament_id: int, team_id: int):
    """Удаляет команду из списка участников турнира"""
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute('SELECT participants FROM tournaments WHERE id=?', (tournament_id,)) as cur:
            row = await cur.fetchone()
            if not row:
                return False

        try:
            participants = json.loads(row[0]) if row[0] else []
        except:
            participants = []

        # Удаляем команду если она есть в списке
        if team_id in participants:
            participants.remove(team_id)

            # Обновляем в БД
            await db.execute('UPDATE tournaments SET participants=? WHERE id=?',
                            (json.dumps(participants), tournament_id))
            await db.commit()
            return True

        return False

async def get_tournament_participants(tournament_id: int):
    """Возвращает список участников турнира с полной информацией о командах"""
    async with aiosqlite.connect(DB_NAME) as db:
        # Получаем участников турнира
        async with db.execute('SELECT participants FROM tournaments WHERE id=?', (tournament_id,)) as cur:
            row = await cur.fetchone()
            if not row:
                return []

        try:
            participant_ids = json.loads(row[0]) if row[0] else []
        except:
            participant_ids = []

        if not participant_ids:
            return []

        # Получаем информацию о командах
        placeholders = ','.join('?' * len(participant_ids))
        query = f'SELECT id, name, tag FROM teams WHERE id IN ({placeholders})'
        async with db.execute(query, participant_ids) as teams_cur:
            teams = [dict(row) for row in await teams_cur.fetchall()]

        # Сортируем в том же порядке, что и в participants
        teams_dict = {team['id']: team for team in teams}
        sorted_teams = [teams_dict.get(team_id) for team_id in participant_ids if team_id in teams_dict]

        return sorted_teams

async def set_tournament_winner(tournament_id: int, place: str, team_id: int):
    """Устанавливает победителя турнира для определенного места"""
    async with aiosqlite.connect(DB_NAME) as db:
        # Получаем текущих победителей
        async with db.execute('SELECT winners FROM tournaments WHERE id=?', (tournament_id,)) as cur:
            row = await cur.fetchone()
            if not row:
                return False

        try:
            winners = json.loads(row[0]) if row[0] else {}
        except:
            winners = {}

        # Устанавливаем победителя для места
        winners[place] = team_id

        # Обновляем в БД
        await db.execute('UPDATE tournaments SET winners=? WHERE id=?',
                        (json.dumps(winners), tournament_id))
        await db.commit()
        return True

async def get_team_rank_alphabetical(team_tag):
    async with aiosqlite.connect(DB_NAME) as db:
        query = 'SELECT COUNT(*) FROM teams WHERE LOWER(tag) < LOWER(?)'
        async with db.execute(query, (team_tag,)) as cursor:
            count_before = (await cursor.fetchone())[0]
            return count_before + 1

# =======================
#   ИГРОКИ
# =======================

async def update_player_metadata(nickname, first_name=None, last_name=None, photo_id=None):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute('SELECT nickname FROM player_metadata WHERE nickname = ?', (nickname,)) as cur:
            exists = await cur.fetchone()

        if exists:
            if first_name is not None: await db.execute('UPDATE player_metadata SET first_name=? WHERE nickname=?', (first_name, nickname))
            if last_name is not None: await db.execute('UPDATE player_metadata SET last_name=? WHERE nickname=?', (last_name, nickname))
            if photo_id is not None: await db.execute('UPDATE player_metadata SET photo_file_id=? WHERE nickname=?', (photo_id, nickname))
        else:
            await db.execute('INSERT INTO player_metadata (nickname, first_name, last_name, photo_file_id) VALUES (?, ?, ?, ?)',
                             (nickname, first_name or "", last_name or "", photo_id))
        await db.commit()

async def get_player_metadata(nickname):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM player_metadata WHERE nickname = ?', (nickname,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else {}

async def perform_player_transfer(player_nickname, old_team_id, new_team_id, date_str):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute('SELECT id, roster, name, tag FROM teams WHERE id=?', (old_team_id,)) as cur:
            old_team_row = await cur.fetchone()
        async with db.execute('SELECT id, roster, name, tag FROM teams WHERE id=?', (new_team_id,)) as cur:
            new_team_row = await cur.fetchone()

        if not old_team_row or not new_team_row:
            return False, "Команда не найдена"

        old_roster = [x.strip() for x in old_team_row[1].split('\n') if x.strip()]
        if player_nickname in old_roster:
            old_roster.remove(player_nickname)
            await db.execute('UPDATE teams SET roster=? WHERE id=?', ("\n".join(old_roster), old_team_id))

        new_roster = [x.strip() for x in new_team_row[1].split('\n') if x.strip()]
        if player_nickname not in new_roster:
            new_roster.append(player_nickname)
            await db.execute('UPDATE teams SET roster=? WHERE id=?', ("\n".join(new_roster), new_team_id))

        old_team_display = f"{old_team_row[2]} [{old_team_row[3]}]"
        new_team_display = f"{new_team_row[2]} [{new_team_row[3]}]"

        await db.execute('INSERT INTO transfers (player_name, old_team, new_team, date) VALUES (?, ?, ?, ?)',
                         (player_nickname, old_team_display, new_team_display, date_str))
        await db.commit()
        return True, f"Переведен в {new_team_display}"

async def update_player_nickname_in_roster(old_nick, new_nick):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('UPDATE player_metadata SET nickname=? WHERE nickname=?', (new_nick, old_nick))
        await db.execute('UPDATE transfers SET player_name=? WHERE player_name=?', (new_nick, old_nick))
        async with db.execute('SELECT id, roster FROM teams') as cur:
            teams = await cur.fetchall()
        for tid, roster in teams:
            lines = [x.strip() for x in roster.split('\n') if x.strip()]
            if old_nick in lines:
                new_lines = [new_nick if x == old_nick else x for x in lines]
                await db.execute('UPDATE teams SET roster=? WHERE id=?', ("\n".join(new_lines), tid))
        await db.commit()

async def get_all_roster_players_paginated(page=0, limit=10):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT roster, name, tag, id FROM teams') as cursor:
            rows = await cursor.fetchall()

    all_players = []
    for row in rows:
        roster_text = row['roster']
        lines = [x.strip() for x in roster_text.split('\n') if x.strip()]
        for p in lines:
            all_players.append({
                'nickname': p,
                'team_name': row['name'],
                'team_tag': row['tag'],
                'team_id': row['id']
            })

    all_players.sort(key=lambda x: x['nickname'].lower())
    total_count = len(all_players)
    total_pages = math.ceil(total_count / limit) if limit > 0 else 1
    start = page * limit
    end = start + limit
    return all_players[start:end], total_pages, total_count, all_players

# --- ОБНОВЛЕННАЯ ФУНКЦИЯ ДЛЯ ДОСТИЖЕНИЙ С ПРИЗОВЫМИ ---
async def get_player_achievements(player_nickname, current_team_id):
    """
    Возвращает список достижений (если текущая команда игрока выигрывала турниры)
    Формат: "🥇 GTC SEASON 1 - 1st (4000 RUB)"
    """
    achievements = []
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        # Ищем турниры с победителями
        async with db.execute("SELECT full_name, season, winners, prize_data FROM tournaments WHERE winners IS NOT NULL AND winners != '{}'") as cur:
            rows = await cur.fetchall()

    for row in rows:
        try:
            winners = json.loads(row['winners'])
            # winners = {"1st": team_id, "2nd": team_id, ...}

            # Разбираем призовой фонд для получения суммы
            prize_data = {}
            if row['prize_data']:
                try: prize_data = json.loads(row['prize_data'])
                except: pass

            dist_raw = prize_data.get('distribution', {})
            if isinstance(dist_raw, dict):
                dist = dist_raw
            elif isinstance(dist_raw, list):
                dist = {str(x.get('place')): str(x.get('amount')) for x in dist_raw if x and x.get('place') is not None}
            else:
                dist = {}
            curr = prize_data.get('currency', '')

            for place, team_id in winners.items():
                if int(team_id) == current_team_id:
                    # Определяем медаль
                    medal = "🏆"
                    if "1" in place: medal = "🥇"
                    elif "2" in place: medal = "🥈"
                    elif "3" in place: medal = "🥉"

                    # Ищем сумму приза для этого места
                    money = dist.get(place, "0")
                    if money != "0" and curr:
                        money_str = f"({money} {curr})"
                    else:
                        money_str = ""

                    ach = f"{medal} {row['full_name']} {row['season']} - {place} {money_str}"
                    achievements.append(ach.strip())
        except:
            continue

    return achievements

async def get_player_stats_and_rank(player_nickname):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM games ORDER BY created_at DESC') as cursor:
            all_games = [dict(row) for row in await cursor.fetchall()]
        async with db.execute('SELECT * FROM transfers WHERE player_name = ?', (player_nickname,)) as cursor:
            transfers = [dict(row) for row in await cursor.fetchall()]

    meta = await get_player_metadata(player_nickname)

    global_scores = {}
    target_stats = {
        'k': 0, 'a': 0, 'd': 0, 'matches': 0, 'rounds': 0, 'r_sum': 0.0,
        'last_games': []
    }

    for game in all_games:
        try:
            s_json = json.loads(game['stats_json'])
            for _, players in s_json.items():
                for p in players:
                    nick = p.get('nickname')
                    if not nick: continue

                    if nick not in global_scores:
                        global_scores[nick] = {'score': 0, 'k':0, 'a':0, 'd':0, 'matches':0, 'r_sum':0}

                    global_scores[nick]['k'] += p.get('K', 0)
                    global_scores[nick]['a'] += p.get('A', 0)
                    global_scores[nick]['d'] += p.get('D', 0)
                    global_scores[nick]['r_sum'] += p.get('RATING', 0.0)
                    global_scores[nick]['matches'] += 1

                    if nick == player_nickname:
                        target_stats['k'] += p.get('K', 0)
                        target_stats['a'] += p.get('A', 0)
                        target_stats['d'] += p.get('D', 0)
                        target_stats['r_sum'] += p.get('RATING', 0.0)
                        target_stats['matches'] += 1
                        target_stats['rounds'] += game['total_rounds']

                        if len(target_stats['last_games']) < 3:
                            t1_tag = game.get('team1_tag') or "?"
                            t2_tag = game.get('team2_tag') or "?"
                            item = f"{game['map_name']} ({game['score_t1']}:{game['score_t2']}) [{t1_tag}] vs [{t2_tag}]"
                            target_stats['last_games'].append(item)
        except: continue

    leaderboard = []
    for nick, s in global_scores.items():
        avg = s['r_sum'] / s['matches'] if s['matches'] > 0 else 0
        score = (s['k'] * 2) + (s['a'] * 1) - (s['d'] * 0.5) + (avg * 100)
        leaderboard.append({'name': nick, 'score': score})

    leaderboard.sort(key=lambda x: x['score'], reverse=True)

    rank = "-"
    player_score = 0
    for idx, item in enumerate(leaderboard):
        if item['name'] == player_nickname:
            rank = idx + 1
            player_score = item['score']
            break

    _, _, _, all_roster = await get_all_roster_players_paginated(0, 99999)
    current_team = "Без команды"
    current_team_id = 0
    for p in all_roster:
        if p['nickname'] == player_nickname:
            current_team = f"{p['team_name']} [{p['team_tag']}]"
            current_team_id = p['team_id']
            break

    rounds = target_stats['rounds'] if target_stats['rounds'] > 0 else 1
    kpr = target_stats['k'] / rounds
    apr = target_stats['a'] / rounds
    dpr = target_stats['d'] / rounds
    svr = (rounds - target_stats['d']) / rounds
    impact = 2.13 * kpr + 0.42 * apr - 0.41
    if impact < 0: impact = 0

    avg_r = target_stats['r_sum'] / target_stats['matches'] if target_stats['matches'] > 0 else 0
    kd = target_stats['k'] / target_stats['d'] if target_stats['d'] > 0 else target_stats['k']

    # Получаем достижения
    achievements = await get_player_achievements(player_nickname, current_team_id)

    return {
        'nickname': player_nickname,
        'first_name': meta.get('first_name', 'Не указано'),
        'last_name': meta.get('last_name', 'Не указано'),
        'photo_id': meta.get('photo_file_id'),
        'kills': target_stats['k'],
        'assists': target_stats['a'],
        'deaths': target_stats['d'],
        'diff': target_stats['k'] - target_stats['d'],
        'helps': target_stats['a'],
        'matches': target_stats['matches'],
        'rounds': target_stats['rounds'],
        'kd': round(kd, 2),
        'kpr': round(kpr, 2),
        'dpr': round(dpr, 2),
        'svr': round(svr, 2),
        'impact': round(impact, 2),
        'avg_rating': round(avg_r, 2),
        'score': round(player_score, 2),
        'rank': rank,
        'current_team': current_team,
        'current_team_id': current_team_id,
        'last_3_games': target_stats['last_games'],
        'transfers': transfers,
        'achievements': achievements
    }

async def get_top_players_list(limit=10):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT stats_json FROM games') as cursor:
            all_games = [dict(row) for row in await cursor.fetchall()]

    scores = {}
    for game in all_games:
        try:
            s_json = json.loads(game['stats_json'])
            for _, players in s_json.items():
                for p in players:
                    n = p.get('nickname')
                    if not n: continue
                    if n not in scores: scores[n] = {'k':0, 'a':0, 'd':0, 'r':0.0, 'm':0}
                    scores[n]['k'] += p.get('K',0)
                    scores[n]['a'] += p.get('A',0)
                    scores[n]['d'] += p.get('D',0)
                    scores[n]['r'] += p.get('RATING',0.0)
                    scores[n]['m'] += 1
        except: continue

    final = []
    for n, s in scores.items():
        avg = s['r']/s['m'] if s['m']>0 else 0
        val = (s['k']*2) + (s['a']*1) - (s['d']*0.5) + (avg*100)
        final.append({'name': n, 'score': round(val, 2)})

    final.sort(key=lambda x: x['score'], reverse=True)
    return final[:limit]

# =======================
#       ТУРНИРЫ
# =======================

async def check_tournament_exists(name):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute('SELECT id FROM tournaments WHERE LOWER(full_name) = LOWER(?)', (name,)) as cursor:
            return True if await cursor.fetchone() else False

async def create_tournament(full_name, season, year, has_qualifiers, has_group_stage, logo_base64, prize_data, mvp_data):
    prize_json = json.dumps(prize_data) if prize_data else None
    mvp_json = json.dumps(mvp_data) if mvp_data else None
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT INTO tournaments
            (full_name, season, year, has_qualifiers, has_group_stage, logo_base64, prize_data, mvp_data, participants, winners)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]', '{}')
        ''', (full_name, season, year, has_qualifiers, has_group_stage, logo_base64, prize_json, mvp_json))
        await db.commit()

async def delete_tournament(tour_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('DELETE FROM tournaments WHERE id = ?', (tour_id,))
        await db.commit()

async def update_tournament_field(tour_id, field, val):
    allowed = ['full_name', 'season', 'year', 'logo_base64', 'prize_data', 'mvp_data']
    if field not in allowed: return False
    if field in ['prize_data', 'mvp_data'] and not isinstance(val, str) and val is not None:
        val = json.dumps(val)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(f'UPDATE tournaments SET {field}=? WHERE id=?', (val, tour_id))
        await db.commit()
    return True

async def get_tournament_by_id(tour_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM tournaments WHERE id = ?', (tour_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

async def get_tournaments_paginated(page=0, limit=3, sort_by='alpha'):
    offset = page * limit
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT COUNT(*) FROM tournaments') as cursor:
            total_count = (await cursor.fetchone())[0]
        if sort_by == 'year': order_sql = "ORDER BY year DESC, full_name ASC"
        else: order_sql = "ORDER BY LOWER(full_name) ASC"
        query = f'SELECT id, full_name, season, year FROM tournaments {order_sql} LIMIT ? OFFSET ?'
        async with db.execute(query, (limit, offset)) as cursor:
            tours = [dict(row) for row in await cursor.fetchall()]
    total_pages = math.ceil(total_count / limit)
    return tours, total_pages, total_count

# =======================
#       ИГРЫ
# =======================

async def add_game_record(tour_id, game_date, game_format, map_name, t1_tag, t2_tag, s1, s2, rounds, stats_dict):
    stats_json = json.dumps(stats_dict)
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute('''
            INSERT INTO games (tournament_id, game_date, game_format, map_name, team1_tag, team2_tag, score_t1, score_t2, total_rounds, stats_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (tour_id, game_date, game_format, map_name, t1_tag, t2_tag, s1, s2, rounds, stats_json))
        new_id = cursor.lastrowid
        await db.commit()
        return new_id

async def get_games_paginated(tour_id, page=0, limit=3, date_filter=None):
    offset = page * limit
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row

        where_sql = "WHERE tournament_id = ?"
        params = [tour_id]
        if date_filter:
            where_sql += " AND game_date = ?"
            params.append(date_filter)

        async with db.execute(f'SELECT COUNT(*) FROM games {where_sql}', tuple(params)) as cur:
            total_count = (await cur.fetchone())[0]

        query = f'SELECT * FROM games {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?'
        params.extend([limit, offset])

        async with db.execute(query, tuple(params)) as cursor:
            games = [dict(row) for row in await cursor.fetchall()]

    total_pages = math.ceil(total_count / limit)
    return games, total_pages, total_count

async def get_game_by_id(game_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM games WHERE id = ?', (game_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

async def delete_game(game_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('DELETE FROM games WHERE id = ?', (game_id,))
        await db.commit()

async def update_game_field(game_id, field, value):
    allowed = ['game_date', 'map_name', 'score_t1', 'score_t2', 'total_rounds']
    if field not in allowed: return False
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(f'UPDATE games SET {field}=? WHERE id=?', (value, game_id))
        await db.commit()
    return True
