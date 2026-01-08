import asyncio
import logging
import base64
import json
import math
import os
import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.session.aiohttp import AiohttpSession

# --- ИМПОРТЫ ИЗ database.py ---
from database import (
    init_db, add_user, check_is_admin, set_admin_role, check_is_owner, remove_admin_role,
    get_admins_paginated, get_user_by_db_id,
    create_team, get_user_info, get_teams_paginated, get_team_by_id, get_team_by_tag,
    delete_team, update_team_field, check_team_exists, get_team_rank_alphabetical,
    create_tournament, check_tournament_exists, get_tournaments_paginated, get_tournament_by_id,
    delete_tournament, update_tournament_field,
    add_game_record, get_games_paginated,
    get_game_by_id, delete_game, update_game_field,
    get_all_roster_players_paginated, get_player_stats_and_rank, get_top_players_list,
    update_player_metadata, perform_player_transfer, update_player_nickname_in_roster,
    add_team_to_tournament, get_tournament_participants, set_tournament_winner
)

from states import (
    AdminTeamCreate, AdminAddAdmin, AdminAddOwner, AdminTeamEdit, 
    TeamListState, TournamentCreate, AdminTourEdit, GameRegister, 
    GameListState, TournamentNav, GameEditState, PlayerAdminState,
    TourAddTeam, TourSetWinner
)

# --- КОНФИГ ---
TOKEN = "8405508314:AAG5mh-RlaLRnTc5Ss2pLGkwcssFbsTbgJY" 

logging.basicConfig(level=logging.INFO)

# Увеличиваем тайм-аут
session = AiohttpSession(timeout=60)
bot = Bot(token=TOKEN, session=session)
dp = Dispatcher()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def escape_md(text):
    if text is None: return ""
    text = str(text)
    # Экранируем все спецсимволы MarkdownV2
    chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in chars:
        text = text.replace(char, f"\\{char}")
    return text

def escape_md_code(text):
    if text is None: return ""
    text = str(text)
    return text.replace('\\', '\\\\').replace('`', '\\`')

async def delete_prev_bot_msg(state: FSMContext):
    data = await state.get_data()
    msg_id = data.get('last_bot_msg_id')
    chat_id = data.get('chat_id')
    if msg_id and chat_id:
        try:
            await bot.delete_message(chat_id, msg_id)
        except:
            pass

def calculate_player_metrics(k, a, d, rounds):
    if rounds == 0: rounds = 1
    kd = k / d if d > 0 else k
    kpr = k / rounds
    helps = a / rounds
    diff = k - d
    svr = (rounds - d) / rounds
    apr = a / rounds
    impact = 2.13 * kpr + 0.42 * apr - 0.41
    if impact < 0: impact = 0
    x = (kd + impact * 1.5) / 2.5
    if x < 0: x = 0
    rating = math.sqrt(x) if x > 0 else 0.0

    return {
        "K": k, "A": a, "D": d,
        "+/-": diff,
        "KPR": round(kpr, 2),
        "DPR": round(d / rounds, 2),
        "SVR": round(svr, 2),
        "IMPACT": round(impact, 2),
        "RATING": round(rating, 2),
        "KD": round(kd, 2),
        "HELPS": a 
    }

def format_game_stats(game, tournament_season=""):
    try:
        stats = json.loads(game['stats_json'])
    except:
        return "⚠️ Ошибка загрузки статистики"

    date_safe = escape_md_code(game['game_date'])
    map_safe = escape_md_code(game['map_name'])
    season_safe = escape_md(tournament_season)
    
    txt = f"⚔️ *Матч ID:* `{game['id']}`\n"
    if season_safe:
        txt += f"❄️ *Сезон:* {season_safe}\n"
    txt += f"📅 `{date_safe}` \\| 🗺 `{map_safe}`\n"
    txt += f"🏆 Счет: *{game['score_t1']} : {game['score_t2']}*\n\n"

    def draw_team_stats(tag, players):
        res = f"🚩 *{escape_md(tag)}*\n"
        res += "```\n" 
        res += f"{'Player':<10} {'K':>2} {'A':>2} {'D':>2} {'KD':>4} {'RTG':>4}\n"
        res += "-"*32 + "\n"
        
        for p in players:
            name = p.get('nickname', 'Player')[:10]
            k = p.get('K', 0)
            a = p.get('A', 0)
            d = p.get('D', 0)
            kd = p.get('KD', 0.0)
            rtg = p.get('RATING', 0.0)
            res += f"{name:<10} {k:>2} {a:>2} {d:>2} {kd:>4} {rtg:>4}\n"
        
        res += "```\n"
        return res

    for team_tag, players_list in stats.items():
        txt += draw_team_stats(team_tag, players_list)

    return txt

# --- КЛАВИАТУРЫ ---

async def get_main_kb(user_id):
    is_admin = await check_is_admin(user_id)
    kb = [
        [InlineKeyboardButton(text="🎨 Создать баннер", callback_data="nav_create_banner")],
        [
            InlineKeyboardButton(text="🛡️ Команды", callback_data="menu_teams_root"),
            InlineKeyboardButton(text="🏆 Турниры", callback_data="menu_tours_root")
        ],
        [
            InlineKeyboardButton(text="🎮 [A] Игры", callback_data="nav_games_main"),
            InlineKeyboardButton(text="👥 Список игроков", callback_data="nav_all_players_list")
        ],
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="nav_profile")]
    ]
    if is_admin:
        kb.append([InlineKeyboardButton(text="⚙️ Админка", callback_data="nav_admin")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_sub_teams_kb(is_admin):
    kb = [
        [InlineKeyboardButton(text="📋 Список команд", callback_data="nav_teams_list")]
    ]
    if is_admin:
        kb.append([InlineKeyboardButton(text="➕ Создать команду", callback_data="admin_create_team")])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_sub_tours_kb(is_admin):
    kb = [
        [InlineKeyboardButton(text="📋 Список турниров", callback_data="nav_tournaments")]
    ]
    if is_admin:
        kb.append([InlineKeyboardButton(text="➕ Создать турнир", callback_data="admin_create_tournament")])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="nav_main")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_games_main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить игру", callback_data="game_add_init")],
        [InlineKeyboardButton(text="📋 Список игр", callback_data="game_list_init")], 
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="nav_main")]
    ])

def get_back_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В главное меню", callback_data="nav_main")]])

def get_back_to_teams_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="menu_teams_root")]])

def get_back_to_tours_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="menu_tours_root")]])

def get_back_to_view_kb(prefix, view_id):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data=f"{prefix}_{view_id}")]])

def get_yes_no_kb(prefix): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Да", callback_data=f"{prefix}_yes"), InlineKeyboardButton(text="Нет", callback_data=f"{prefix}_no")]])

def get_currency_kb(prefix):
    curs = ["RUB", "EUR", "USD", "UAH", "G", "USDT", "TON"]
    kb = []; row = []
    for cur in curs:
        row.append(InlineKeyboardButton(text=cur, callback_data=f"{prefix}_{cur}"))
        if len(row)==3: kb.append(row); row=[]
    if row: kb.append(row)
    if prefix=="tour_fund": kb.append([InlineKeyboardButton(text="❌ НЕТУ ФОНДА", callback_data=f"{prefix}_NONE")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_format_kb():
    formats = ["5x5", "4x4", "3x3", "2x2", "1x1"]
    kb = []
    for f in formats:
        kb.append([InlineKeyboardButton(text=f"⚔️ {f}", callback_data=f"set_format_{f}")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_tournament_select_kb(index, total, t_id):
    nav_row = []
    if index > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️", callback_data=TournamentNav(action="prev", index=index-1, id=0).pack()))
    else:
        nav_row.append(InlineKeyboardButton(text="⏺", callback_data="ignore"))

    nav_row.append(InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="ignore"))

    if index < total - 1:
        nav_row.append(InlineKeyboardButton(text="➡️", callback_data=TournamentNav(action="next", index=index+1, id=0).pack()))
    else:
        nav_row.append(InlineKeyboardButton(text="⏺", callback_data="ignore"))

    kb = [
        nav_row,
        [InlineKeyboardButton(text="✅ Выбрать этот турнир", callback_data=TournamentNav(action="select", index=index, id=t_id).pack())],
        [InlineKeyboardButton(text="🔙 Отмена", callback_data="nav_games_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_teams_carousel_kb(teams, page, total_pages, current_sort):
    kb = []
    for team in teams: kb.append([InlineKeyboardButton(text=f"{team['name']} [{team['tag']}]", callback_data=f"view_team_{team['id']}")])
    nav = []
    if page>0: nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"team_page_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"📄 {page+1}/{max(1,total_pages)}", callback_data="ignore"))
    if page<total_pages-1: nav.append(InlineKeyboardButton(text="➡️", callback_data=f"team_page_{page+1}"))
    kb.append(nav)
    txt = "🔤 По Имени" if current_sort=='tag' else "🏷 По Тегу"
    srt = 'name' if current_sort=='tag' else 'tag'
    kb.append([InlineKeyboardButton(text=f"🔍 Сорт: {txt}", callback_data=f"set_sort_{srt}")])
    kb.append([InlineKeyboardButton(text="🔙 В меню", callback_data="menu_teams_root")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_tournaments_carousel_kb(tours, page, total_pages, current_sort):
    kb = []
    for tour in tours: kb.append([InlineKeyboardButton(text=f"{tour['full_name']} ({tour['year']})", callback_data=f"view_tour_{tour['id']}")])
    nav = []
    if page>0: nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"tour_page_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"📄 {page+1}/{max(1,total_pages)}", callback_data="ignore"))
    if page<total_pages-1: nav.append(InlineKeyboardButton(text="➡️", callback_data=f"tour_page_{page+1}"))
    kb.append(nav)
    txt = "🔤 По Алфавиту" if current_sort=='alpha' else "📅 По Году"
    srt = 'year' if current_sort=='alpha' else 'alpha'
    kb.append([InlineKeyboardButton(text=f"🔍 Сорт: {txt}", callback_data=f"set_toursort_{srt}")])
    kb.append([InlineKeyboardButton(text="🔙 В меню", callback_data="menu_tours_root")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_games_carousel_kb(games, page, total_pages, tour_id):
    kb = []
    for game in games:
        btn_text = f"{game['team1_tag']} vs {game['team2_tag']} ({game['game_date']})"
        kb.append([InlineKeyboardButton(text=btn_text, callback_data=f"view_game_{game['id']}")])
    nav = []
    if page>0: nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"game_page_{tour_id}_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"📄 {page+1}/{max(1,total_pages)}", callback_data="ignore"))
    if page<total_pages-1: nav.append(InlineKeyboardButton(text="➡️", callback_data=f"game_page_{tour_id}_{page+1}"))
    kb.append(nav)
    
    kb.append([InlineKeyboardButton(text="📅 Фильтр по дате", callback_data=f"filter_games_date_{tour_id}")])
    kb.append([InlineKeyboardButton(text="🔙 К выбору турнира", callback_data=f"game_list_init")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_admins_carousel_kb(admins, page, total_pages):
    kb = []
    for adm in admins:
        role_icon = "👑" if adm['is_admin'] == 2 else "👮‍♂️"
        kb.append([InlineKeyboardButton(text=f"{role_icon} {adm['username']}", callback_data=f"view_admin_{adm['user_id']}")])
    nav = []
    if page>0: nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin_page_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"📄 {page+1}/{max(1,total_pages)}", callback_data="ignore"))
    if page<total_pages-1: nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin_page_{page+1}"))
    kb.append(nav)
    kb.append([InlineKeyboardButton(text="🔙 В меню", callback_data="nav_main")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_all_roster_players_kb(players, page, total_pages):
    kb = []
    for p in players:
        nick = p['nickname']
        safe_nick = nick[:30] 
        kb.append([InlineKeyboardButton(text=f"👤 {nick}", callback_data=f"roster_view_{safe_nick}")])
        
    nav = []
    if page>0: nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"roster_page_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"📄 {page+1}/{max(1,total_pages)}", callback_data="ignore"))
    if page<total_pages-1: nav.append(InlineKeyboardButton(text="➡️", callback_data=f"roster_page_{page+1}"))
    kb.append(nav)
    
    kb.append([InlineKeyboardButton(text="🏆 Топ-100", callback_data="roster_top_100_0")])
    kb.append([InlineKeyboardButton(text="🔙 В меню", callback_data="nav_main")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_map_select_kb(mode="reg", game_id=None):
    maps = [
        ("🏜️ Sandstone", "Sandstone"),
        ("🏘️ Province", "Province"),
        ("🏭 Rust", "Rust"),
        ("☢️ Zone 7", "Zone 7"),
        ("🌸 Hanami", "Hanami"),
        ("🏖️ Breeze", "Breeze"),
        ("🐫 Dune", "Dune"),
        ("🏯 Sakura", "Sakura")
    ]
    kb = []
    row = []
    for name, value in maps:
        if mode == "reg":
            cb_data = f"set_reg_map_{value}"
        else:
            cb_data = f"set_edit_map_{game_id}_{value}"
        row.append(InlineKeyboardButton(text=name, callback_data=cb_data))
        if len(row) == 2:
            kb.append(row)
            row = []
    if row:
        kb.append(row)
    if mode == "edit":
        kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data=f"view_game_{game_id}")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

# =======================
#    СТАРТ И МЕНЮ
# =======================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await add_user(message.from_user.id, message.from_user.username)
    kb = await get_main_kb(message.from_user.id)
    await message.answer(f"👋 Привет, *{escape_md(message.from_user.first_name)}*\\!", reply_markup=kb, parse_mode="MarkdownV2")

@dp.callback_query(F.data == "nav_main")
async def nav_main(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    kb = await get_main_kb(callback.from_user.id)
    try: await callback.message.edit_text("🏠 *Главное меню:*", reply_markup=kb, parse_mode="MarkdownV2")
    except TelegramBadRequest: 
        await callback.message.delete()
        await callback.message.answer("🏠 *Главное меню:*", reply_markup=kb, parse_mode="MarkdownV2")

@dp.callback_query(F.data == "nav_profile")
async def nav_profile(callback: types.CallbackQuery):
    u = await get_user_info(callback.from_user.id)
    r_map = {0: "Пользователь 👤", 1: "Администратор 👮‍♂️", 2: "Владелец 👑"}
    role = r_map.get(u['is_admin'], "Неизвестно")
    text = f"👤 *Личный кабинет*\n\n🆔 *ID:* `{u['user_id']}`\n📛 *Username:* @{escape_md(u['username'])}\n🏷 *Роль:* {escape_md(role)}"
    if u['is_admin'] > 0: text += f"\n🤝 *Добавил:* {escape_md(u['promoted_by'] if u['promoted_by'] else 'Система')}"
    try: await callback.message.edit_text(text, reply_markup=get_back_kb(), parse_mode="MarkdownV2")
    except TelegramBadRequest: 
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=get_back_kb(), parse_mode="MarkdownV2")

# ==========================================
#    НОВЫЕ ПРОМЕЖУТОЧНЫЕ МЕНЮ
# ==========================================

@dp.callback_query(F.data == "menu_teams_root")
async def menu_teams_root(callback: types.CallbackQuery):
    is_admin = await check_is_admin(callback.from_user.id)
    kb = get_sub_teams_kb(is_admin)
    try: await callback.message.edit_text("🛡️ *Управление командами*\nВыберите действие:", reply_markup=kb, parse_mode="MarkdownV2")
    except TelegramBadRequest:
        await callback.message.delete()
        await callback.message.answer("🛡️ *Управление командами*\nВыберите действие:", reply_markup=kb, parse_mode="MarkdownV2")

@dp.callback_query(F.data == "menu_tours_root")
async def menu_tours_root(callback: types.CallbackQuery):
    is_admin = await check_is_admin(callback.from_user.id)
    kb = get_sub_tours_kb(is_admin)
    try: await callback.message.edit_text("🏆 *Управление турнирами*\nВыберите действие:", reply_markup=kb, parse_mode="MarkdownV2")
    except TelegramBadRequest:
        await callback.message.delete()
        await callback.message.answer("🏆 *Управление турнирами*\nВыберите действие:", reply_markup=kb, parse_mode="MarkdownV2")

# ==========================================
#    СПИСОК ИГРОКОВ (ИЗ СОСТАВОВ)
# ==========================================

@dp.callback_query(F.data == "nav_all_players_list")
async def nav_all_players_start(callback: types.CallbackQuery):
    await show_all_roster_players_page(callback, 0)

@dp.callback_query(F.data.startswith("roster_page_"))
async def nav_roster_players_pagination(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[-1])
    await show_all_roster_players_page(callback, page)

async def show_all_roster_players_page(callback: types.CallbackQuery, page):
    players, pages, count, _ = await get_all_roster_players_paginated(page, 10)
    text = f"👥 *Список зарегистрированных игроков* \\(Всего: {count}\\)"
    kb = get_all_roster_players_kb(players, page, pages)
    
    try: await callback.message.edit_text(text, reply_markup=kb, parse_mode="MarkdownV2")
    except TelegramBadRequest:
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=kb, parse_mode="MarkdownV2")

# --- ПРОСМОТР ПРОФИЛЯ ИГРОКА ---
@dp.callback_query(F.data.startswith("roster_view_"))
async def view_roster_player_profile(callback: types.CallbackQuery):
    nickname = callback.data.replace("roster_view_", "")
    
    stats = await get_player_stats_and_rank(nickname)
    
    full_name = stats['last_name'] + " \"" + stats['nickname'] + "\" " + stats['first_name']
    header = f"👤 *Игрок:* {escape_md(full_name)}\n"
    
    team_txt = f"🛡️ *Команда:* {escape_md(stats['current_team'])}\n"
    rank_txt = f"🏆 *Ранг:* \\#{escape_md(stats['rank'])} \\(Очки: {escape_md(stats['score'])}\\)\n"
    
    main_stats = (
        f"📊 *Статистика:*\n"
        f"🔫 K: {stats['kills']} \\| A: {stats['assists']} \\| D: {stats['deaths']}\n"
        f"➕/➖: {escape_md(stats['diff'])} \\| Helps: {escape_md(stats['helps'])}\n"
        f"💀 KD: {escape_md(stats['kd'])}\n"
        f"🔫 KPR: {escape_md(stats['kpr'])} \\| 🛡 DPR: {escape_md(stats['dpr'])}\n"
        f"❤️ SVR: {escape_md(stats['svr'])}\n"
        f"💥 IMPACT: {escape_md(stats['impact'])}\n"
        f"⭐ RATING: {escape_md(stats['avg_rating'])}\n"
    )
    
    last_games_txt = "\n📅 *Последние 3 игры:*\n"
    if stats['last_3_games']:
        for g in stats['last_3_games']:
            last_games_txt += f"▫️ {escape_md(g)}\n"
    else:
        last_games_txt += "▫️ Нет сыгранных игр\n"
        
    achievements_txt = "\n🏅 *Достижения:*\n"
    if stats['achievements']:
        for ach in stats['achievements']:
            achievements_txt += f"{escape_md(ach)}\n"
    else:
        achievements_txt += "▫️ Нет\n"

    transfers_txt = "\n🔄 *История трансферов:*\n"
    if stats['transfers']:
        for t in stats['transfers']:
            old = escape_md(t['old_team'])
            new = escape_md(t['new_team'])
            date = escape_md(t['date'])
            transfers_txt += f"▫️ {date}: {old} ➡️ {new}\n"
    else:
        transfers_txt += "▫️ Пусто\n"
        
    full_text = header + team_txt + rank_txt + "\n" + main_stats + last_games_txt + achievements_txt + transfers_txt
    
    kb_rows = []
    if await check_is_admin(callback.from_user.id):
        safe_nick = nickname[:20] 
        kb_rows.append([InlineKeyboardButton(text="✏️ Изм. Имя/Фамилию", callback_data=f"adm_p_name_{safe_nick}")])
        kb_rows.append([InlineKeyboardButton(text="✏️ Изм. Ник", callback_data=f"adm_p_nick_{safe_nick}")])
        kb_rows.append([InlineKeyboardButton(text="🔄 Трансфер", callback_data=f"adm_p_trans_{safe_nick}")])

    kb_rows.append([InlineKeyboardButton(text="🔙 К списку", callback_data="nav_all_players_list")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    
    photo_path = "assets/photo.png"
    
    try: await callback.message.delete()
    except: pass
    
    if os.path.exists(photo_path):
        photo = FSInputFile(photo_path)
        await callback.message.answer_photo(photo, caption=full_text, reply_markup=kb, parse_mode="MarkdownV2")
    else:
        await callback.message.answer(full_text, reply_markup=kb, parse_mode="MarkdownV2")

# --- РЕДАКТИРОВАНИЕ ИГРОКА И ТРАНСФЕРЫ ---
@dp.callback_query(F.data.startswith("adm_p_name_"))
async def admin_edit_player_name(callback: types.CallbackQuery, state: FSMContext):
    nick = callback.data.replace("adm_p_name_", "")
    await state.update_data(target_player_nick=nick)
    msg = await callback.message.answer("✏️ Введите новое *Имя* и *Фамилию* через пробел \\(например `Ivan Ivanov`\\):", parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id, chat_id=callback.message.chat.id)
    await state.set_state(PlayerAdminState.waiting_for_new_name)

@dp.message(PlayerAdminState.waiting_for_new_name)
async def admin_save_player_name(message: types.Message, state: FSMContext):
    try: await message.delete()
    except: pass
    await delete_prev_bot_msg(state)
    
    data = await state.get_data()
    nick = data['target_player_nick']
    
    parts = message.text.split()
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ""
    
    await update_player_metadata(nick, first_name=first_name, last_name=last_name)
    
    fake_cb = types.CallbackQuery(id='0', from_user=message.from_user, chat_instance='0', message=message, data=f"roster_view_{nick}")
    await view_roster_player_profile(fake_cb)
    
    cnf = await message.answer("✅ Данные обновлены!")
    await asyncio.sleep(2)
    try: await cnf.delete()
    except: pass
    await state.clear()

@dp.callback_query(F.data.startswith("adm_p_nick_"))
async def admin_edit_player_nick(callback: types.CallbackQuery, state: FSMContext):
    nick = callback.data.replace("adm_p_nick_", "")
    await state.update_data(target_player_nick=nick)
    msg = await callback.message.answer("✏️ Введите новый *Никнейм* \\(Внимание: статистика старого ника останется привязанной к старому имени в истории игр\\):", parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id, chat_id=callback.message.chat.id)
    await state.set_state(PlayerAdminState.waiting_for_new_nick)

@dp.message(PlayerAdminState.waiting_for_new_nick)
async def admin_save_player_nick(message: types.Message, state: FSMContext):
    try: await message.delete()
    except: pass
    await delete_prev_bot_msg(state)
    
    data = await state.get_data()
    old_nick = data['target_player_nick']
    new_nick = message.text.strip()
    
    await update_player_nickname_in_roster(old_nick, new_nick)
    
    fake_cb = types.CallbackQuery(id='0', from_user=message.from_user, chat_instance='0', message=message, data=f"roster_view_{new_nick}")
    await view_roster_player_profile(fake_cb)
    
    cnf = await message.answer("✅ Никнейм обновлен в составах!")
    await asyncio.sleep(2)
    try: await cnf.delete()
    except: pass
    await state.clear()

@dp.callback_query(F.data.startswith("adm_p_trans_"))
async def admin_transfer_start(callback: types.CallbackQuery, state: FSMContext):
    nick = callback.data.replace("adm_p_trans_", "")
    await state.update_data(target_player_nick=nick)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сделать свободным агентом (FFT)", callback_data="trans_fft")],
        [InlineKeyboardButton(text="Перевести в команду...", callback_data="trans_team_select")],
        [InlineKeyboardButton(text="Отмена", callback_data=f"roster_view_{nick}")]
    ])
    try: await callback.message.edit_caption(caption="🔄 Выберите тип трансфера:", reply_markup=kb)
    except: 
        await callback.message.delete()
        await callback.message.answer("🔄 Выберите тип трансфера:", reply_markup=kb)

@dp.callback_query(F.data == "trans_fft")
async def admin_transfer_fft(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    nick = data['target_player_nick']
    
    stats = await get_player_stats_and_rank(nick)
    old_team_id = stats.get('current_team_id', 0)
    
    from database import get_team_by_tag
    fft_team = await get_team_by_tag("FFT")
    if not fft_team:
        await callback.answer("Ошибка: Команда FFT не найдена", show_alert=True)
        return
    
    if old_team_id == fft_team['id']:
        await callback.answer("Игрок уже свободный агент", show_alert=True)
        return
        
    date_str = datetime.date.today().strftime("%Y.%m.%d")
    success, msg = await perform_player_transfer(nick, old_team_id, fft_team['id'], date_str)
    
    if success:
        await callback.answer("Успешно!")
        fake_cb = types.CallbackQuery(id='0', from_user=callback.from_user, chat_instance='0', message=callback.message, data=f"roster_view_{nick}")
        await view_roster_player_profile(fake_cb)
    else:
        await callback.answer(f"Ошибка: {msg}", show_alert=True)

@dp.callback_query(F.data == "trans_team_select")
async def admin_transfer_select_team(callback: types.CallbackQuery, state: FSMContext):
    await show_transfer_teams_page(callback, 0)
    await state.set_state(PlayerAdminState.selecting_transfer_team)

async def show_transfer_teams_page(callback: types.CallbackQuery, page):
    from database import get_teams_paginated
    teams, pages, count = await get_teams_paginated(page, 5, 'tag')
    
    kb = []
    for t in teams:
        kb.append([InlineKeyboardButton(text=f"{t['name']} [{t['tag']}]", callback_data=f"do_trans_{t['id']}")])
        
    nav = []
    if page>0: nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"trans_page_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"📄 {page+1}/{max(1,pages)}", callback_data="ignore"))
    if page<pages-1: nav.append(InlineKeyboardButton(text="➡️", callback_data=f"trans_page_{page+1}"))
    kb.append(nav)
    
    try: await callback.message.edit_caption(caption="Выберите новую команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except: 
        await callback.message.delete()
        await callback.message.answer("Выберите новую команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("trans_page_"))
async def admin_transfer_pagination(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[-1])
    await show_transfer_teams_page(callback, page)

@dp.callback_query(F.data.startswith("do_trans_"))
async def admin_transfer_execute(callback: types.CallbackQuery, state: FSMContext):
    new_team_id = int(callback.data.split("_")[-1])
    data = await state.get_data()
    nick = data['target_player_nick']
    
    stats = await get_player_stats_and_rank(nick)
    old_team_id = stats.get('current_team_id', 0)
    
    if old_team_id == new_team_id:
        await callback.answer("Игрок уже в этой команде", show_alert=True)
        return
        
    date_str = datetime.date.today().strftime("%Y.%m.%d")
    success, msg = await perform_player_transfer(nick, old_team_id, new_team_id, date_str)
    
    if success:
        await callback.answer("Трансфер успешен!")
        fake_cb = types.CallbackQuery(id='0', from_user=callback.from_user, chat_instance='0', message=callback.message, data=f"roster_view_{nick}")
        await view_roster_player_profile(fake_cb)
        await state.clear()
    else:
        await callback.answer(f"Ошибка: {msg}", show_alert=True)

# --- ТОП ИГРОКОВ (С ПЛЕЙСХОЛДЕРАМИ ДО 100) ---
@dp.callback_query(F.data.startswith("roster_top_100_"))
async def show_top_players(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    page = int(parts[-1])
    
    top_100 = await get_top_players_list(100)
    
    PAGE_SIZE = 10
    total_pages = 10 
    
    start_idx = page * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    
    text = f"🏆 *Топ 100 игроков* \\(Стр\\. {page+1}/{total_pages}\\)\n\n"
    
    for i in range(start_idx, end_idx):
        position = i + 1
        medal = "🥇" if i==0 else "🥈" if i==1 else "🥉" if i==2 else f"{position}\\."
        
        if i < len(top_100):
            p = top_100[i]
            p_name = escape_md(p['name'])
            p_score = escape_md(p['score'])
            text += f"{medal} *{p_name}* — {p_score} pts\n"
        else:
            text += f"{medal} \\#\n"
            
    kb = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️", callback_data=f"roster_top_100_{page-1}"))
    nav_row.append(InlineKeyboardButton(text=f"📄 {page+1}", callback_data="ignore"))
    if page < total_pages - 1:
         nav_row.append(InlineKeyboardButton(text="➡️", callback_data=f"roster_top_100_{page+1}"))
    kb.append(nav_row)
    
    kb.append([InlineKeyboardButton(text="🔙 К списку", callback_data="nav_all_players_list")])
    
    try: await callback.message.edit_caption(caption=text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="MarkdownV2")
    except:
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="MarkdownV2")

# ==========================================
#    АДМИНКА: ПЕРСОНАЛ
# ==========================================

@dp.callback_query(F.data == "nav_admin")
async def nav_admin(callback: types.CallbackQuery):
    if not await check_is_admin(callback.from_user.id): return
    is_owner = await check_is_owner(callback.from_user.id)
    kb_rows = []
    if is_owner:
        kb_rows.append([InlineKeyboardButton(text="➕ Добавить Админа", callback_data="admin_add_role_1")])
        kb_rows.append([InlineKeyboardButton(text="➕ Добавить Владельца", callback_data="admin_add_role_2")])
    kb_rows.append([InlineKeyboardButton(text="👥 Список персонала", callback_data="admin_list_start")])
    kb_rows.append([InlineKeyboardButton(text="🔙 В меню", callback_data="nav_main")])
    try: await callback.message.edit_text("⚙️ *Админка*", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="MarkdownV2")
    except TelegramBadRequest: await callback.message.delete(); await callback.message.answer("⚙️ *Админка*", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="MarkdownV2")

@dp.callback_query(F.data.startswith("admin_add_role_"))
async def start_add_any_admin(callback: types.CallbackQuery, state: FSMContext):
    if not await check_is_owner(callback.from_user.id): return
    role_level = int(callback.data.split("_")[-1])
    role_name = "Админа" if role_level == 1 else "Владельца"
    await callback.message.edit_text(f"✍️ Введите *Username* нового {role_name}:", reply_markup=get_back_kb(), parse_mode="MarkdownV2")
    if role_level == 1: await state.set_state(AdminAddAdmin.waiting_for_username)
    else: await state.set_state(AdminAddOwner.waiting_for_username)

@dp.message(AdminAddAdmin.waiting_for_username)
async def process_add_admin_lvl1(message: types.Message, state: FSMContext):
    t = message.text; await set_admin_role(t, f"@{message.from_user.username}", 1)
    await message.answer(f"✅ Пользователь *{escape_md(t)}* теперь Администратор\\.", reply_markup=await get_main_kb(message.from_user.id), parse_mode="MarkdownV2")
    await state.clear()

@dp.message(AdminAddOwner.waiting_for_username)
async def process_add_admin_lvl2(message: types.Message, state: FSMContext):
    t = message.text; await set_admin_role(t, f"@{message.from_user.username}", 2)
    await message.answer(f"✅ Пользователь *{escape_md(t)}* теперь Владелец\\.", reply_markup=await get_main_kb(message.from_user.id), parse_mode="MarkdownV2")
    await state.clear()

@dp.callback_query(F.data == "admin_list_start")
async def admin_list_start(callback: types.CallbackQuery):
    await show_admins_page(callback, 0)

@dp.callback_query(F.data.startswith("admin_page_"))
async def admin_list_pagination(callback: types.CallbackQuery):
    await show_admins_page(callback, int(callback.data.split("_")[-1]))

async def show_admins_page(callback: types.CallbackQuery, page):
    admins, pages, count = await get_admins_paginated(page, 5)
    text = f"👥 *Список персонала* \\(Всего: {count}\\)"
    try: await callback.message.edit_text(text, reply_markup=get_admins_carousel_kb(admins, page, pages), parse_mode="MarkdownV2")
    except TelegramBadRequest:
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=get_admins_carousel_kb(admins, page, pages), parse_mode="MarkdownV2")

@dp.callback_query(F.data.startswith("view_admin_"))
async def view_specific_admin(callback: types.CallbackQuery):
    target_id = int(callback.data.split("_")[-1]); viewer_id = callback.from_user.id
    target_user = await get_user_by_db_id(target_id)
    if not target_user: await callback.answer("Пользователь не найден", show_alert=True); return
    is_viewer_owner = await check_is_owner(viewer_id)
    r_map = {1: "Администратор 👮‍♂️", 2: "Владелец 👑"}
    role_str = r_map.get(target_user['is_admin'], "Неизвестно")
    info = f"👤 *Профиль сотрудника*\n\n📛 *Ник:* {escape_md(target_user['username'])}\n🏷 *Роль:* {escape_md(role_str)}\n🤝 *Назначил:* {escape_md(target_user['promoted_by'])}"
    kb_rows = []
    if is_viewer_owner and target_id != viewer_id:
        kb_rows.append([InlineKeyboardButton(text="🗑 УДАЛИТЬ ИЗ ПЕРСОНАЛА", callback_data=f"del_admin_confirm_{target_id}")])
    kb_rows.append([InlineKeyboardButton(text="🔙 Назад к списку", callback_data="admin_list_start")])
    try: await callback.message.edit_text(info, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="MarkdownV2")
    except TelegramBadRequest:
        await callback.message.delete()
        await callback.message.answer(info, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="MarkdownV2")

@dp.callback_query(F.data.startswith("del_admin_confirm_"))
async def delete_admin_handler(callback: types.CallbackQuery):
    if not await check_is_owner(callback.from_user.id):
        await callback.answer("❌ У вас нет прав!", show_alert=True); return
    target_id = int(callback.data.split("_")[-1])
    await remove_admin_role(target_id)
    await callback.answer("✅ Сотрудник разжалован!", show_alert=True)
    await admin_list_start(callback)

# ==========================================
#    АДМИНКА: КОМАНДЫ (СОЗДАНИЕ И РЕДАКТИРОВАНИЕ)
# ==========================================

@dp.callback_query(F.data == "admin_create_team")
async def admin_team_start(callback: types.CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id): return
    msg = await callback.message.edit_text("⚡ *Создание команды*\n\n1️⃣ Введите название команды:", reply_markup=get_back_to_teams_kb(), parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id, chat_id=callback.message.chat.id)
    await state.set_state(AdminTeamCreate.waiting_for_name)

@dp.message(AdminTeamCreate.waiting_for_name)
async def admin_team_name(message: types.Message, state: FSMContext):
    await message.delete() 
    await delete_prev_bot_msg(state) 
    await state.update_data(name=message.text)
    msg = await message.answer("2️⃣ Введите короткий *ТЕГ* команды (например: `NAVI`, `C9`):", reply_markup=get_back_to_teams_kb(), parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id)
    await state.set_state(AdminTeamCreate.waiting_for_tag)

@dp.message(AdminTeamCreate.waiting_for_tag)
async def admin_team_tag(message: types.Message, state: FSMContext):
    await message.delete()
    tag = message.text.strip()
    if await check_team_exists("temp", tag):
        await delete_prev_bot_msg(state)
        msg = await message.answer("❌ Команда с таким тегом уже есть! Придумайте другой:", reply_markup=get_back_to_teams_kb(), parse_mode="MarkdownV2")
        await state.update_data(last_bot_msg_id=msg.message_id)
        return
    await delete_prev_bot_msg(state)
    await state.update_data(tag=tag)
    msg = await message.answer("3️⃣ Введите *состав команды* (каждый ник с новой строки):", reply_markup=get_back_to_teams_kb(), parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id)
    await state.set_state(AdminTeamCreate.waiting_for_roster)

@dp.message(AdminTeamCreate.waiting_for_roster)
async def admin_team_roster(message: types.Message, state: FSMContext):
    await message.delete()
    await delete_prev_bot_msg(state)
    await state.update_data(roster=message.text)
    msg = await message.answer("4️⃣ Отправьте *Логотип* команды (картинку):", reply_markup=get_back_to_teams_kb(), parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id)
    await state.set_state(AdminTeamCreate.waiting_for_logo)

@dp.message(AdminTeamCreate.waiting_for_logo, F.photo)
async def admin_team_logo(message: types.Message, state: FSMContext):
    await message.delete()
    await delete_prev_bot_msg(state)
    photo = message.photo[-1]
    file_info = await bot.get_file(photo.file_id)
    downloaded_file = await bot.download_file(file_info.file_path)
    logo_base64 = base64.b64encode(downloaded_file.read()).decode('utf-8')
    data = await state.get_data()
    await create_team(data['name'], data['tag'], data['roster'], logo_base64)
    await message.answer(f"✅ Команда *{escape_md(data['name'])}* \\[{escape_md(data['tag'])}\\] успешно создана\\!", parse_mode="MarkdownV2", reply_markup=await get_main_kb(message.from_user.id))
    await state.clear()

# --- ПРОСМОТР И РЕДАКТИРОВАНИЕ КОМАНД ---
@dp.callback_query(F.data == "nav_teams_list")
async def nav_teams_list_start(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(team_sort_mode='tag')
    await show_teams_page(callback, 0, state)

@dp.callback_query(F.data.startswith("team_page_"))
async def nav_teams_pagination(callback: types.CallbackQuery, state: FSMContext):
    await show_teams_page(callback, int(callback.data.split("_")[-1]), state)

@dp.callback_query(F.data.startswith("set_sort_"))
async def change_team_sort(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(team_sort_mode=callback.data.split("_")[-1])
    await show_teams_page(callback, 0, state)

async def show_teams_page(callback: types.CallbackQuery, page, state: FSMContext):
    data = await state.get_data(); sort = data.get('team_sort_mode', 'tag')
    teams, pages, count = await get_teams_paginated(page, 3, sort)
    if count == 0:
        await callback.message.edit_text("🛡️ *Список команд пуст\\.*", reply_markup=get_back_kb(), parse_mode="MarkdownV2")
        return
    mode_text = "По Тегу" if sort == 'tag' else "По Имени"
    text = f"🛡️ *Список команд* \\(Всего: {count}\\)\n🗂 Сортировка: _{escape_md(mode_text)}_"
    try: await callback.message.edit_text(text, reply_markup=get_teams_carousel_kb(teams, page, pages, sort), parse_mode="MarkdownV2")
    except TelegramBadRequest: 
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=get_teams_carousel_kb(teams, page, pages, sort), parse_mode="MarkdownV2")

@dp.callback_query(F.data.startswith("view_team_"))
async def view_specific_team(callback: types.CallbackQuery):
    tid = int(callback.data.split("_")[-1]); team = await get_team_by_id(tid)
    if not team: await callback.answer("Команда не найдена", show_alert=True); return
    
    rank = await get_team_rank_alphabetical(team['tag'])
    roster_display = "\n".join([f"• {escape_md(p)}" for p in team['roster'].split('\n') if p.strip()])
    info = f"🛡️ *Команда:* {escape_md(team['name'])}\n🏷 *Тег:* `{escape_md(team['tag'])}`\n📊 *Ранг:* #{rank}\n\n👥 *Состав:*\n{roster_display}"
    
    kb_rows = []
    if await check_is_admin(callback.from_user.id):
        kb_rows.append([InlineKeyboardButton(text="✏️ Имя", callback_data=f"edit_team_name_{tid}"), InlineKeyboardButton(text="✏️ Тег", callback_data=f"edit_team_tag_{tid}")])
        kb_rows.append([InlineKeyboardButton(text="👥 Состав", callback_data=f"edit_team_roster_{tid}"), InlineKeyboardButton(text="🖼️ Лого", callback_data=f"edit_team_logo_base64_{tid}")])
        kb_rows.append([InlineKeyboardButton(text="❌ УДАЛИТЬ", callback_data=f"del_team_confirm_{tid}")])
    kb_rows.append([InlineKeyboardButton(text="🔙 К списку", callback_data="nav_teams_list")])

    try:
        await callback.message.delete()
        await callback.message.answer_photo(BufferedInputFile(base64.b64decode(team['logo_base64']), filename="l.png"), caption=info, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="MarkdownV2")
    except Exception as e: 
        if "message to delete not found" in str(e):
             await callback.message.answer_photo(BufferedInputFile(base64.b64decode(team['logo_base64']), filename="l.png"), caption=info, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="MarkdownV2")
        else:
             await callback.message.answer(escape_md(f"Ошибка: {e}") + "\n\n" + info, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="MarkdownV2")

@dp.callback_query(F.data.startswith("del_team_confirm_"))
async def delete_team_handler(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if not await check_is_admin(uid): return
    await delete_team(int(callback.data.split("_")[-1]))
    await callback.message.delete() 
    await callback.message.answer("🗑️ Команда удалена!\nВы перемещены в главное меню.", reply_markup=await get_main_kb(uid))

# Хендлеры редактирования команды
@dp.callback_query(F.data.startswith("edit_team_"))
async def edit_team_start(callback: types.CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id): return
    parts = callback.data.split("_")
    field = parts[2] # name, tag, roster
    tid = int(parts[-1])
    
    if field == "logo": 
        # Логотип (base64) обрабатывается отдельно
        await callback.message.answer("🖼️ Отправьте новое *Логотип* (картинку):", parse_mode="MarkdownV2")
        await state.update_data(edit_team_id=tid, edit_field="logo_base64")
        await state.set_state(AdminTeamEdit.waiting_for_new_value)
        return

    text_map = {"name": "название", "tag": "тег", "roster": "состав"}
    await callback.message.answer(f"✏️ Введите новое *{text_map.get(field, field)}*:", parse_mode="MarkdownV2")
    await state.update_data(edit_team_id=tid, edit_field=field)
    await state.set_state(AdminTeamEdit.waiting_for_new_value)

@dp.message(AdminTeamEdit.waiting_for_new_value)
async def edit_team_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tid = data['edit_team_id']
    field = data['edit_field']
    
    val = None
    if field == "logo_base64":
        if not message.photo:
            await message.answer("❌ Это не фото!")
            return
        photo = message.photo[-1]
        file_info = await bot.get_file(photo.file_id)
        downloaded_file = await bot.download_file(file_info.file_path)
        val = base64.b64encode(downloaded_file.read()).decode('utf-8')
    else:
        val = message.text
        
    await update_team_field(tid, field, val)
    await message.answer("✅ Обновлено!")
    
    await message.answer("Используйте меню, чтобы вернуться к списку.", reply_markup=get_sub_teams_kb(True))
    await state.clear()


# ==========================================
#    АДМИНКА: ТУРНИРЫ (СОЗДАНИЕ И РЕДАКТИРОВАНИЕ)
# ==========================================

@dp.callback_query(F.data == "admin_create_tournament")
async def admin_tour_start(callback: types.CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id): return
    msg = await callback.message.edit_text("🏆 *Создание турнира*\n\n1️⃣ Введите полное название турнира:", reply_markup=get_back_to_tours_kb(), parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id, chat_id=callback.message.chat.id)
    await state.set_state(TournamentCreate.waiting_for_tour_name)

@dp.message(TournamentCreate.waiting_for_tour_name)
async def admin_tour_name(message: types.Message, state: FSMContext):
    await message.delete()
    await delete_prev_bot_msg(state)
    await state.update_data(full_name=message.text)
    msg = await message.answer("2️⃣ Введите название *сезона* \\(например `Season 1`\\):", reply_markup=get_back_to_tours_kb(), parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id)
    await state.set_state(TournamentCreate.waiting_for_tour_season)

@dp.message(TournamentCreate.waiting_for_tour_season)
async def admin_tour_season(message: types.Message, state: FSMContext):
    await message.delete()
    await delete_prev_bot_msg(state)
    await state.update_data(season=message.text)
    msg = await message.answer("3️⃣ Введите *Год* проведения \\(число\\):", reply_markup=get_back_to_tours_kb(), parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id)
    await state.set_state(TournamentCreate.waiting_for_year)

@dp.message(TournamentCreate.waiting_for_year)
async def admin_tour_year(message: types.Message, state: FSMContext):
    await message.delete()
    if not message.text.isdigit():
        await delete_prev_bot_msg(state)
        msg = await message.answer("❌ Введите число!", reply_markup=get_back_to_tours_kb(), parse_mode="MarkdownV2")
        await state.update_data(last_bot_msg_id=msg.message_id)
        return
    await delete_prev_bot_msg(state)
    await state.update_data(year=int(message.text))
    kb = get_yes_no_kb("qualifiers")
    msg = await message.answer("4️⃣ Есть ли Квалификации?", reply_markup=kb, parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id)
    await state.set_state(TournamentCreate.waiting_for_qualifiers)

@dp.callback_query(TournamentCreate.waiting_for_qualifiers)
async def admin_tour_qual(callback: types.CallbackQuery, state: FSMContext):
    ans = True if "yes" in callback.data else False
    await state.update_data(has_qualifiers=ans)
    kb = get_yes_no_kb("groups")
    await callback.message.edit_text("5️⃣ Есть ли Групповой этап?", reply_markup=kb, parse_mode="MarkdownV2")
    await state.set_state(TournamentCreate.waiting_for_group_stage)

@dp.callback_query(TournamentCreate.waiting_for_group_stage)
async def admin_tour_group(callback: types.CallbackQuery, state: FSMContext):
    ans = True if "yes" in callback.data else False
    await state.update_data(has_group_stage=ans)
    await callback.message.delete()
    msg = await callback.message.answer("6️⃣ Отправьте *Логотип* турнира \\(картинку\\):", reply_markup=get_back_to_tours_kb(), parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id)
    await state.set_state(TournamentCreate.waiting_for_logo)

@dp.message(TournamentCreate.waiting_for_logo, F.photo)
async def admin_tour_logo(message: types.Message, state: FSMContext):
    await message.delete()
    await delete_prev_bot_msg(state)
    photo = message.photo[-1]
    file_info = await bot.get_file(photo.file_id)
    downloaded_file = await bot.download_file(file_info.file_path)
    logo_base64 = base64.b64encode(downloaded_file.read()).decode('utf-8')
    await state.update_data(logo_base64=logo_base64)
    msg = await message.answer("💰 Выберите валюту призового фонда:", reply_markup=get_currency_kb("tour_fund"), parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id)
    await state.set_state(TournamentCreate.waiting_for_prize_currency)

@dp.callback_query(TournamentCreate.waiting_for_prize_currency)
async def admin_tour_p_curr(callback: types.CallbackQuery, state: FSMContext):
    curr = callback.data.split("_")[-1]
    if curr == "NONE":
        await state.update_data(prize_data=None, mvp_data=None)
        await finish_create_tournament(callback.message, state)
    else:
        await state.update_data(p_curr=curr)
        await callback.message.edit_text(f"💰 Введите общую сумму фонда \\({curr}\\):", reply_markup=get_back_to_tours_kb(), parse_mode="MarkdownV2")
        await state.set_state(TournamentCreate.waiting_for_prize_total)

@dp.message(TournamentCreate.waiting_for_prize_total)
async def admin_tour_p_total(message: types.Message, state: FSMContext):
    await message.delete()
    await delete_prev_bot_msg(state)
    await state.update_data(p_total=message.text)
    kb = get_yes_no_kb("mvp_dec")
    msg = await message.answer("⭐ Будет ли приз MVP?", reply_markup=kb, parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id)
    await state.set_state(TournamentCreate.waiting_for_mvp_decision)

@dp.callback_query(TournamentCreate.waiting_for_mvp_decision)
async def admin_tour_mvp_ask(callback: types.CallbackQuery, state: FSMContext):
    if "no" in callback.data:
        await finalize_prize_data(state)
        await state.update_data(mvp_data=None)
        await finish_create_tournament(callback.message, state)
    else:
        await callback.message.edit_text("⭐ Введите сумму награды MVP:", reply_markup=get_back_to_tours_kb(), parse_mode="MarkdownV2")
        await state.set_state(TournamentCreate.waiting_for_mvp_amount)

@dp.message(TournamentCreate.waiting_for_mvp_amount)
async def admin_tour_mvp_val(message: types.Message, state: FSMContext):
    await message.delete()
    await delete_prev_bot_msg(state)
    data = await state.get_data()
    mvp_struct = {"amount": message.text, "currency": data['p_curr']}
    await state.update_data(mvp_data=mvp_struct)
    await finalize_prize_data(state)
    await finish_create_tournament(message, state)

async def finalize_prize_data(state: FSMContext):
    d = await state.get_data()
    p_struct = {"total": d['p_total'], "currency": d['p_curr'], "distribution": {"1st": "Winner", "2nd": "Runner-up"}} 
    await state.update_data(prize_data=p_struct)

async def finish_create_tournament(message: types.Message, state: FSMContext):
    data = await state.get_data()
    safe_name = escape_md(data['full_name'])
    text = f"✅ Турнир *{safe_name}* успешно создан\\!"
    kb = await get_main_kb(message.from_user.id)

    try:
        await create_tournament(
            data['full_name'], data.get('season', ''), data['year'], data['has_qualifiers'], data['has_group_stage'],
            data['logo_base64'], data.get('prize_data'), data.get('mvp_data')
        )
        try:
            await message.edit_text(text, parse_mode="MarkdownV2", reply_markup=kb)
        except Exception:
            await message.answer(text, parse_mode="MarkdownV2", reply_markup=kb)
            
    except Exception as e:
        err_msg = escape_md(f"Ошибка: {e}")
        await message.answer(f"❌ {err_msg}", parse_mode="MarkdownV2")
    
    await state.clear()

# --- ПРОСМОТР ТУРНИРОВ И УПРАВЛЕНИЕ УЧАСТНИКАМИ/ПОБЕДИТЕЛЯМИ ---
@dp.callback_query(F.data == "nav_tournaments")
async def nav_tournaments_start(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(tour_sort_mode='alpha'); await show_tours_page(callback, 0, state)

@dp.callback_query(F.data.startswith("tour_page_"))
async def nav_tours_pagination(callback: types.CallbackQuery, state: FSMContext):
    await show_tours_page(callback, int(callback.data.split("_")[-1]), state)

@dp.callback_query(F.data.startswith("set_toursort_"))
async def change_tour_sort(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(tour_sort_mode=callback.data.split("_")[-1]); await show_tours_page(callback, 0, state)

async def show_tours_page(callback: types.CallbackQuery, page, state: FSMContext):
    data = await state.get_data(); sort = data.get('tour_sort_mode', 'alpha')
    tours, pages, count = await get_tournaments_paginated(page, 3, sort)
    if count == 0:
        await callback.message.edit_text("🏆 *Список турниров пуст\\.*", reply_markup=get_back_kb(), parse_mode="MarkdownV2")
        return
    mode_text = "По Алфавиту" if sort == 'alpha' else "По Году"
    text = f"🏆 *Список турниров* \\(Всего: {count}\\)\n🗂 Сортировка: _{escape_md(mode_text)}_"
    try: await callback.message.edit_text(text, reply_markup=get_tournaments_carousel_kb(tours, page, pages, sort), parse_mode="MarkdownV2")
    except TelegramBadRequest: 
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=get_tournaments_carousel_kb(tours, page, pages, sort), parse_mode="MarkdownV2")

@dp.callback_query(F.data.startswith("view_tour_"))
async def view_specific_tour(callback: types.CallbackQuery):
    tid = int(callback.data.split("_")[-1])
    # Импортируем внутри функции или используем глобальный импорт, если он есть
    from database import get_tournament_by_id
    tour = await get_tournament_by_id(tid)
    
    if not tour: 
        await callback.answer("Турнир не найден", show_alert=True)
        return

    # Обработка призового фонда
    try: 
        pdata = json.loads(tour['prize_data'])
    except: 
        pdata = None
    
    # Обработка MVP
    try: 
        mdata = json.loads(tour['mvp_data'])
    except: 
        mdata = None

    # Формирование строки призового фонда
    p_str = "Нет фонда"
    if pdata:
        # Важно: экранируем валюту и значения
        curr = pdata.get('currency', '?')
        total = pdata.get('total', 0)
        dist = pdata.get('distribution', {})
        
        # Генерация списка мест с экранированием ключей (например, "3-4") и значений
        pl_str = "\n".join([f"   🏅 {escape_md(k)} место: {escape_md(v)} {escape_md(curr)}" for k, v in dist.items()])
        p_str = f"*{escape_md(total)} {escape_md(curr)}*\n{pl_str}"

    # Формирование строки MVP
    m_str = "Нет"
    if mdata: 
        amount = mdata.get('amount', '0')
        currency = mdata.get('currency', '')
        m_str = f"{escape_md(amount)} {escape_md(currency)}"

    # Формирование этапов
    stg = []
    if tour['has_qualifiers']: stg.append("Квалификации")
    if tour['has_group_stage']: stg.append("Групповой этап")
    stg.append("Плей-офф (Main)") # Здесь обычные дефисы и скобки
    
    # Экранируем каждый этап и соединяем экранированной стрелочкой
    # escape_md превратит "Плей-офф (Main)" в "Плей\-офф \(Main\)"
    stg_escaped = [escape_md(s) for s in stg]
    stg_str = " \\-\\> ".join(stg_escaped)

    # Сезон
    season_txt = f"❄️ *Сезон:* {escape_md(tour['season'])}\n" if tour['season'] else ""
    
    # Подсчет участников
    try: 
        parts = json.loads(tour['participants'])
    except: 
        parts = []
    parts_count = len(parts)

    # Итоговый текст
    info = (
        f"🏆 *Турнир:* {escape_md(tour['full_name'])}\n"
        f"{season_txt}"
        f"📅 *Год:* {tour['year']}\n"
        f"🚦 *Этапы:* {stg_str}\n"
        f"👥 *Участников:* {parts_count}\n\n"
        f"💰 *Призовой фонд:*\n{p_str}\n\n"
        f"⭐ *MVP Приз:* {m_str}"
    )

    kb_rows = []
    if await check_is_admin(callback.from_user.id):
        # Кнопки управления участниками и победителями
        kb_rows.append([
            InlineKeyboardButton(text="➕ Участники", callback_data=f"add_team_to_tour_{tid}"), 
            InlineKeyboardButton(text="🏆 Победители", callback_data=f"set_winner_tour_{tid}")
        ])
        kb_rows.append([
            InlineKeyboardButton(text="✏️ Название/Сезон", callback_data=f"edit_tour_full_name_{tid}"), 
            InlineKeyboardButton(text="✏️ Год", callback_data=f"edit_tour_year_{tid}")
        ])
        kb_rows.append([
            InlineKeyboardButton(text="💰 Приз Фонд", callback_data=f"edit_tour_prize_data_{tid}"), 
            InlineKeyboardButton(text="⭐ MVP", callback_data=f"edit_tour_mvp_data_{tid}")
        ])
        kb_rows.append([
            InlineKeyboardButton(text="🖼️ Лого", callback_data=f"edit_tour_logo_base64_{tid}"), 
            InlineKeyboardButton(text="❌ УДАЛИТЬ", callback_data=f"del_tour_confirm_{tid}")
        ])
        
    kb_rows.append([InlineKeyboardButton(text="🔙 К списку", callback_data="nav_tournaments")])
    
    try:
        await callback.message.delete()
        # Отправка фото с подписью
        await callback.message.answer_photo(
            BufferedInputFile(base64.b64decode(tour['logo_base64']), filename="l.png"), 
            caption=info, 
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), 
            parse_mode="MarkdownV2"
        )
    except Exception as e: 
        # Если ошибка (например, слишком длинный текст или битая картинка), отправляем текстом
        err_msg = escape_md(f"Ошибка отображения: {e}")
        # Если картинка битая, отправляем просто текст
        await callback.message.answer(
            err_msg + "\n\n" + info, 
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), 
            parse_mode="MarkdownV2"
        )

# --- ДОБАВЛЕНИЕ УЧАСТНИКОВ В ТУРНИР ---
@dp.callback_query(F.data.startswith("add_team_to_tour_"))
async def add_tour_team_start(callback: types.CallbackQuery, state: FSMContext):
    tid = int(callback.data.split("_")[-1])
    await state.update_data(target_tour_id=tid)
    msg = await callback.message.answer("✍️ Введите *ТЕГ* команды, которую нужно добавить в турнир:", parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id, chat_id=callback.message.chat.id)
    await state.set_state(TourAddTeam.waiting_for_tag)

@dp.message(TourAddTeam.waiting_for_tag)
async def add_tour_team_process(message: types.Message, state: FSMContext):
    await message.delete()
    await delete_prev_bot_msg(state)
    
    tag = message.text.strip()
    data = await state.get_data()
    tid = data['target_tour_id']
    
    # Ищем команду
    from database import get_team_by_tag, add_team_to_tournament
    team = await get_team_by_tag(tag)
    
    if not team:
        await message.answer(f"❌ Команда с тегом `{escape_md(tag)}` не найдена в базе данных\\. Проверьте тег или зарегистрируйте команду\\.", parse_mode="MarkdownV2")
    else:
        success = await add_team_to_tournament(tid, team['id'])
        if success:
            await message.answer(f"✅ Команда *{escape_md(team['name'])}* добавлена в турнир!", parse_mode="MarkdownV2")
        else:
            await message.answer(f"⚠️ Команда уже участвует в этом турнире.", parse_mode="MarkdownV2")
            
    # Возврат к турниру
    fake_cb = types.CallbackQuery(id='0', from_user=message.from_user, chat_instance='0', message=message, data=f"view_tour_{tid}")
    await view_specific_tour(fake_cb)
    await state.clear()

# --- ВЫБОР ПОБЕДИТЕЛЯ ТУРНИРА ---
@dp.callback_query(F.data.startswith("set_winner_tour_"))
async def set_tour_winner_start(callback: types.CallbackQuery, state: FSMContext):
    tid = int(callback.data.split("_")[-1])
    await state.update_data(target_tour_id=tid)
    
    # Получаем турнир и призы
    from database import get_tournament_by_id
    tour = await get_tournament_by_id(tid)
    prize_data = {}
    if tour['prize_data']:
        try: prize_data = json.loads(tour['prize_data'])
        except: pass
    
    dist = prize_data.get('distribution', {})
    
    kb = []
    # Если есть распределение призов, создаем кнопки по ключам (1st, 2nd...)
    if dist:
        for place in dist.keys():
             kb.append([InlineKeyboardButton(text=f"🏅 {place}", callback_data=f"win_place_{place}")])
    else:
        # Стандартные кнопки
        kb.append([InlineKeyboardButton(text="🥇 1 Место", callback_data="win_place_1st")])
        kb.append([InlineKeyboardButton(text="🥈 2 Место", callback_data="win_place_2nd")])
        kb.append([InlineKeyboardButton(text="🥉 3 Место", callback_data="win_place_3rd")])

    kb.append([InlineKeyboardButton(text="🔙 Отмена", callback_data=f"view_tour_{tid}")])
    await callback.message.answer("🏆 Выберите, какое место вы хотите назначить:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("win_place_"))
async def set_tour_winner_place(callback: types.CallbackQuery, state: FSMContext):
    place = callback.data.replace("win_place_", "")
    await state.update_data(target_place=place)
    
    # Показываем список участников
    data = await state.get_data()
    tid = data['target_tour_id']
    
    from database import get_tournament_participants
    teams = await get_tournament_participants(tid)
    
    if not teams:
        await callback.message.edit_text("❌ В этом турнире нет участников! Сначала добавьте команды.")
        return

    kb = []
    for t in teams:
        kb.append([InlineKeyboardButton(text=f"{t['name']} [{t['tag']}]", callback_data=f"confirm_winner_{t['id']}")])
    
    kb.append([InlineKeyboardButton(text="🔙 Отмена", callback_data=f"view_tour_{tid}")])
    
    await callback.message.edit_text(f"🏆 Выберите команду, занявшую *{place}* место:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="MarkdownV2")
    await state.set_state(TourSetWinner.selecting_team)

@dp.callback_query(F.data.startswith("confirm_winner_"))
async def set_tour_winner_confirm(callback: types.CallbackQuery, state: FSMContext):
    team_id = int(callback.data.split("_")[-1])
    data = await state.get_data()
    tid = data['target_tour_id']
    place = data['target_place']
    
    from database import set_tournament_winner
    await set_tournament_winner(tid, place, team_id)
    
    await callback.answer(f"✅ Победитель ({place}) установлен!")
    
    # Возврат
    fake_cb = types.CallbackQuery(id='0', from_user=callback.from_user, chat_instance='0', message=callback.message, data=f"view_tour_{tid}")
    await view_specific_tour(fake_cb)
    await state.clear()

@dp.callback_query(F.data.startswith("del_tour_confirm_"))
async def delete_tour_handler(callback: types.CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    await delete_tournament(int(callback.data.split("_")[-1]))
    await callback.message.delete() 
    await callback.message.answer("🗑️ Турнир удален!\nВы перемещены в главное меню.", reply_markup=await get_main_kb(uid))

# Хендлеры редактирования турнира
# Хендлеры редактирования турнира
@dp.callback_query(F.data.startswith("edit_tour_"))
async def edit_tour_start(callback: types.CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id): return
    parts = callback.data.split("_")
    
    # Структура callback: edit_tour_{FIELD}_{ID}
    # Но FIELD может содержать подчеркивания (например, full_name, prize_data)
    # Поэтому ID берем с конца, а поле собираем из середины
    tid = int(parts[-1])
    field_parts = parts[2:-1]
    field = "_".join(field_parts)
    
    if field == "logo_base64":
        await callback.message.answer("🖼️ Отправьте новый *Логотип* \\(картинку\\):", parse_mode="MarkdownV2")
    else:
        # ИСПРАВЛЕНИЕ: Экранируем название поля, так как в нем могут быть "_"
        safe_field = escape_md(field)
        await callback.message.answer(f"✏️ Введите новое значение для *{safe_field}*:", parse_mode="MarkdownV2")
        
    await state.update_data(edit_tour_id=tid, edit_field=field)
    await state.set_state(AdminTourEdit.waiting_for_new_value)

@dp.message(AdminTourEdit.waiting_for_new_value)
async def edit_tour_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tid = data['edit_tour_id']
    field = data['edit_field']
    
    val = None
    if field == "logo_base64":
        if not message.photo:
            await message.answer("❌ Это не фото!")
            return
        photo = message.photo[-1]
        file_info = await bot.get_file(photo.file_id)
        downloaded_file = await bot.download_file(file_info.file_path)
        val = base64.b64encode(downloaded_file.read()).decode('utf-8')
    else:
        val = message.text
        if field == 'year' and not val.isdigit():
             await message.answer("❌ Год должен быть числом!")
             return
    
    await update_tournament_field(tid, field, val)
    await message.answer("✅ Турнир обновлен! Вернитесь в меню.", reply_markup=get_sub_tours_kb(True))
    await state.clear()

# =======================
#    МЕНЮ ИГР (ФУНКЦИОНАЛ НА МЕСТЕ)
# =======================

@dp.callback_query(F.data == "nav_games_main")
async def nav_games_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🎮 *Управление играми*\n\nВыберите действие:",
        reply_markup=get_games_main_kb(),
        parse_mode="MarkdownV2"
    )

# --- ВЫБОР ТУРНИРА ---
async def start_tournament_selection(callback: types.CallbackQuery, state: FSMContext, next_state_obj):
    tours, _, _ = await get_tournaments_paginated(0, 100, 'year')
    if not tours:
        await callback.answer("Нет активных турниров!", show_alert=True)
        return
    await state.update_data(tournaments_cache=tours)
    await show_tour_select_page(callback, 0, state)
    await state.set_state(next_state_obj)

@dp.callback_query(F.data == "game_add_init")
async def game_add_init(callback: types.CallbackQuery, state: FSMContext):
    await start_tournament_selection(callback, state, GameRegister.selecting_tournament)

@dp.callback_query(F.data == "game_list_init")
async def game_list_init(callback: types.CallbackQuery, state: FSMContext):
    await start_tournament_selection(callback, state, GameListState.selecting_tournament_for_list)

async def show_tour_select_page(callback: types.CallbackQuery, index: int, state: FSMContext):
    data = await state.get_data()
    tours = data.get('tournaments_cache', [])
    if not tours: return
    t = tours[index]
    total = len(tours)
    text = (f"🏆 *Выберите турнир:*\n\n📌 Название: *{escape_md(t['full_name'])}*\n📅 Год: {t['year']}\n🆔 ID: `{t['id']}`")
    kb = get_tournament_select_kb(index, total, t['id'])
    try: await callback.message.edit_text(text, reply_markup=kb, parse_mode="MarkdownV2")
    except TelegramBadRequest: pass

@dp.callback_query(TournamentNav.filter(F.action.in_({"prev", "next"})))
async def navigate_tour_select(callback: types.CallbackQuery, callback_data: TournamentNav, state: FSMContext):
    await show_tour_select_page(callback, callback_data.index, state)
    await callback.answer()

@dp.callback_query(TournamentNav.filter(F.action == "select"))
async def select_tour_done(callback: types.CallbackQuery, callback_data: TournamentNav, state: FSMContext):
    tid = callback_data.id
    current_state = await state.get_state()
    
    if current_state == GameRegister.selecting_tournament:
        await state.update_data(reg_game_tour_id=tid)
        await callback.message.edit_text(
            f"✅ Турнир ID {tid} выбран\\.\n⚔️ Выберите *формат* игры:", 
            reply_markup=get_format_kb(), 
            parse_mode="MarkdownV2"
        )
        await state.set_state(GameRegister.waiting_for_format)
    
    elif current_state == GameListState.selecting_tournament_for_list:
        await state.update_data(current_tour_id=tid, date_filter=None)
        await show_games_page(callback, 0, state)
    else:
        await callback.answer("Ошибка состояния", show_alert=True)

# ==================================
#    РЕГИСТРАЦИЯ ИГРЫ
# ==================================

@dp.callback_query(GameRegister.waiting_for_format)
async def game_reg_format(callback: types.CallbackQuery, state: FSMContext):
    fmt = callback.data.split("_")[-1] 
    await state.update_data(game_format=fmt)
    await callback.message.delete()
    
    # Отправляем сообщение и сохраняем ID для последующего редактирования
    msg = await callback.message.answer("📅 Введите *дату* игры в формате `YYYY.MM.DD`\nПример: `2024.05.20`", parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id, chat_id=callback.message.chat.id)
    await state.set_state(GameRegister.waiting_for_date)

@dp.message(GameRegister.waiting_for_date)
async def game_reg_date(message: types.Message, state: FSMContext):
    await message.delete() # Удаляем сообщение пользователя
    if len(message.text) < 8:
        # При ошибке редактируем предыдущее сообщение бота
        await delete_prev_bot_msg(state) 
        msg = await message.answer("❌ Неверный формат даты\\! Попробуйте еще раз:", parse_mode="MarkdownV2")
        await state.update_data(last_bot_msg_id=msg.message_id)
        return
    
    await delete_prev_bot_msg(state) # Удаляем вопрос про дату
    await state.update_data(game_date=message.text)
    
    msg = await message.answer("3️⃣ Выберите *карту*:", reply_markup=get_map_select_kb(mode="reg"), parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id)
    await state.set_state(GameRegister.waiting_for_map)

# ОБРАБОТЧИК КНОПОК КАРТЫ ПРИ РЕГИСТРАЦИИ
@dp.callback_query(F.data.startswith("set_reg_map_"), GameRegister.waiting_for_map)
async def game_reg_map_btn(callback: types.CallbackQuery, state: FSMContext):
    map_name = callback.data.replace("set_reg_map_", "")
    await state.update_data(map_name=map_name)
    
    # Редактируем сообщение с кнопками на ввод счета
    await callback.message.edit_text(f"✅ Карта: *{escape_md(map_name)}*\n\n🔢 Введите *счет* \\(например `13-11`\\):", parse_mode="MarkdownV2")
    # Обновляем last_bot_msg_id, хотя он тот же, на всякий случай
    await state.update_data(last_bot_msg_id=callback.message.message_id)
    await state.set_state(GameRegister.waiting_for_score)

# ОБРАБОТЧИК СЧЕТА
@dp.message(GameRegister.waiting_for_score)
async def game_reg_score(message: types.Message, state: FSMContext):
    await message.delete()
    try:
        parts = message.text.split('-')
        if len(parts) != 2: raise ValueError
        s1, s2 = map(int, parts)
        rounds = s1 + s2
        await state.update_data(s1=s1, s2=s2, rounds=rounds)
        
        await delete_prev_bot_msg(state)
        msg = await message.answer(f"✅ Счет: {s1}:{s2}\n\n4️⃣ Введите *ТЕГ* первой команды \\(команда должна быть зарегистрирована\\):", parse_mode="MarkdownV2")
        await state.update_data(last_bot_msg_id=msg.message_id)
        await state.set_state(GameRegister.waiting_for_team1_tag)
    except:
        await delete_prev_bot_msg(state)
        msg = await message.answer("❌ Ошибка формата\\! Используйте: `13-11`", parse_mode="MarkdownV2")
        await state.update_data(last_bot_msg_id=msg.message_id)

@dp.message(GameRegister.waiting_for_team1_tag)
async def game_reg_t1_tag(message: types.Message, state: FSMContext):
    await message.delete()
    tag = message.text.strip()
    team = await get_team_by_tag(tag)
    
    if not team:
        await delete_prev_bot_msg(state)
        msg = await message.answer("❌ Команда не найдена\\! Введите существующий тег:", parse_mode="MarkdownV2")
        await state.update_data(last_bot_msg_id=msg.message_id)
        return

    roster_raw = team['roster']
    roster_list = [name.strip() for name in roster_raw.split('\n') if name.strip()]
    if not roster_list:
        await delete_prev_bot_msg(state)
        msg = await message.answer("❌ У этой команды пустой состав\\!", parse_mode="MarkdownV2")
        await state.update_data(last_bot_msg_id=msg.message_id)
        return

    await state.update_data(
        t1_tag=tag,
        current_roster=roster_list,
        current_team_idx=1,
        current_stats=[], 
        current_player_idx=0
    )
    # Тут мы не удаляем сообщение, а редактируем его в ask_next_player_stats
    await ask_next_player_stats(message, state)

async def ask_next_player_stats(message: types.Message, state: FSMContext):
    data = await state.get_data()
    roster = data['current_roster']
    idx = data['current_player_idx']
    
    # Получаем ID сообщения для редактирования
    msg_id = data.get('last_bot_msg_id')
    chat_id = message.chat.id

    # Если прошли всех игроков команды
    if idx >= len(roster):
        if data['current_team_idx'] == 1:
            await state.update_data(t1_stats_final=data['current_stats'])
            text = "✅ Статистика Команды 1 сохранена\\.\n\n5️⃣ Введите *ТЕГ* второй команды:"
            
            # Редактируем старое сообщение
            try:
                await bot.edit_message_text(text=text, chat_id=chat_id, message_id=msg_id, parse_mode="MarkdownV2")
            except:
                msg = await bot.send_message(chat_id, text, parse_mode="MarkdownV2")
                await state.update_data(last_bot_msg_id=msg.message_id)
                
            await state.set_state(GameRegister.waiting_for_team2_tag)
        else:
            await finish_game_registration(message, state)
        return

    player_name = roster[idx]
    counter_str = f"\\({idx + 1}/{len(roster)}\\)"
    text = f"📊 {counter_str} Введите статистику для игрока *{escape_md(player_name)}*\nФормат: `K A D` \\(например `15 4 10`\\)"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Не участвовал", callback_data="player_dnp")]])
    
    # Пытаемся редактировать, если не выйдет - отправляем новое
    try:
        await bot.edit_message_text(text=text, chat_id=chat_id, message_id=msg_id, reply_markup=kb, parse_mode="MarkdownV2")
    except:
        msg = await bot.send_message(chat_id, text, reply_markup=kb, parse_mode="MarkdownV2")
        await state.update_data(last_bot_msg_id=msg.message_id)
        
    await state.set_state(GameRegister.waiting_for_player_stats)

@dp.callback_query(GameRegister.waiting_for_player_stats, F.data == "player_dnp")
async def process_player_dnp(callback: types.CallbackQuery, state: FSMContext):
    # Не удаляем сообщение, так как оно будет отредактировано в ask_next_player_stats
    data = await state.get_data()
    new_idx = data['current_player_idx'] + 1
    await state.update_data(current_player_idx=new_idx)
    await ask_next_player_stats(callback.message, state)

@dp.message(GameRegister.waiting_for_player_stats)
async def process_player_stats_text(message: types.Message, state: FSMContext):
    await message.delete() # Удаляем ответ юзера
    try:
        parts = message.text.split()
        if len(parts) != 3:
            # Просто повторяем запрос (в реальном боте можно мигнуть ошибкой)
            await ask_next_player_stats(message, state) 
            return
            
        k, a, d = map(int, parts)
        
        data = await state.get_data()
        current_roster = data['current_roster']
        idx = data['current_player_idx']
        player_name = current_roster[idx]
        rounds = data['rounds']
        
        metrics = calculate_player_metrics(k, a, d, rounds)
        metrics['nickname'] = player_name
        
        stats_list = data['current_stats']
        stats_list.append(metrics)
        await state.update_data(current_stats=stats_list)
        
        await state.update_data(current_player_idx=idx + 1)
        await ask_next_player_stats(message, state)
        
    except ValueError:
        await ask_next_player_stats(message, state)

@dp.message(GameRegister.waiting_for_team2_tag)
async def game_reg_t2_tag(message: types.Message, state: FSMContext):
    await message.delete()
    tag = message.text.strip()
    data = await state.get_data()
    
    # Получаем ID для редактирования
    msg_id = data.get('last_bot_msg_id')
    chat_id = message.chat.id

    if tag.lower() == data['t1_tag'].lower():
        try: await bot.edit_message_text(text="❌ Команды должны быть разными\\! Введите другой тег:", chat_id=chat_id, message_id=msg_id, parse_mode="MarkdownV2")
        except: pass
        return

    team = await get_team_by_tag(tag)
    if not team:
        try: await bot.edit_message_text(text="❌ Команда не найдена\\! Введите существующий тег:", chat_id=chat_id, message_id=msg_id, parse_mode="MarkdownV2")
        except: pass
        return

    roster_list = [name.strip() for name in team['roster'].split('\n') if name.strip()]
    if not roster_list:
        try: await bot.edit_message_text(text="❌ Пустой состав\\!", chat_id=chat_id, message_id=msg_id, parse_mode="MarkdownV2")
        except: pass
        return

    await state.update_data(
        t2_tag=tag,
        current_roster=roster_list,
        current_team_idx=2,
        current_stats=[], 
        current_player_idx=0
    )
    await ask_next_player_stats(message, state)

async def finish_game_registration(message: types.Message, state: FSMContext):
    data = await state.get_data()
    full_stats = {data['t1_tag']: data['t1_stats_final'], data['t2_tag']: data['current_stats']}
    
    # Удаляем последнее сообщение бота (форма ввода)
    await delete_prev_bot_msg(state)
    
    try:
        game_id = await add_game_record(
            data['reg_game_tour_id'], 
            data['game_date'],
            data['game_format'],
            data['map_name'], 
            data['t1_tag'], 
            data['t2_tag'], 
            data['s1'], 
            data['s2'], 
            data['rounds'], 
            full_stats
        )
        
        formatted_id = f"{game_id:09}"
        await message.answer(
            f"✅ *Игра успешно сохранена\\!*\n🆔 ID: `{formatted_id}`\n📅 {escape_md(data['game_date'])}\n🗺 {escape_md(data['map_name'])} \\({data['s1']}:{data['s2']}\\)", 
            reply_markup=await get_main_kb(message.from_user.id), 
            parse_mode="MarkdownV2"
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка сохранения: {escape_md(str(e))}", parse_mode="MarkdownV2")
    
    await state.clear()

# ==========================================
#    ПРОСМОТР, УДАЛЕНИЕ И РЕДАКТИРОВАНИЕ
# ==========================================

@dp.callback_query(F.data.startswith("view_game_"))
async def view_game_handler(callback: types.CallbackQuery, state: FSMContext):
    game_id = int(callback.data.split("_")[-1])
    game = await get_game_by_id(game_id)
    if not game:
        await callback.answer("Игра не найдена!", show_alert=True)
        return

    # Получаем сезон турнира
    from database import get_tournament_by_id
    tour = await get_tournament_by_id(game['tournament_id'])
    season_name = tour['season'] if tour else ""
    
    text = format_game_stats(game, season_name)
    
    kb_rows = []
    
    if await check_is_admin(callback.from_user.id):
        kb_rows.append([
            InlineKeyboardButton(text="✏️ Дату", callback_data=f"edit_game_date_{game_id}"),
            InlineKeyboardButton(text="✏️ Карту", callback_data=f"edit_game_map_{game_id}"),
            InlineKeyboardButton(text="✏️ Счет", callback_data=f"edit_game_score_{game_id}")
        ])
        kb_rows.append([InlineKeyboardButton(text="❌ УДАЛИТЬ ИГРУ", callback_data=f"del_game_confirm_{game_id}")])
    
    kb_rows.append([InlineKeyboardButton(text="🔙 К списку игр", callback_data=f"list_games_{game['tournament_id']}")])

    try:
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="MarkdownV2")
    except TelegramBadRequest:
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="MarkdownV2")

@dp.callback_query(F.data.startswith("del_game_confirm_"))
async def delete_game_handler(callback: types.CallbackQuery):
    if not await check_is_admin(callback.from_user.id): return
    game_id = int(callback.data.split("_")[-1])
    game = await get_game_by_id(game_id)
    tour_id = game['tournament_id'] if game else 0
    await delete_game(game_id)
    await callback.answer("✅ Игра удалена", show_alert=True)
    if tour_id:
        await callback.message.edit_text("🗑 Игра удалена.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 К списку", callback_data=f"list_games_{tour_id}")]]))
    else:
        await callback.message.edit_text("Игра удалена", reply_markup=get_back_kb())

@dp.callback_query(F.data.startswith("edit_game_date_"))
async def edit_game_date_start(callback: types.CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id): return
    gid = int(callback.data.split("_")[-1])
    await state.update_data(edit_game_id=gid)
    msg = await callback.message.edit_text("✏️ Введите новую *дату* \\(YYYY\\.MM\\.DD\\):", reply_markup=get_back_to_view_kb("view_game", gid), parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id, chat_id=callback.message.chat.id)
    await state.set_state(GameEditState.waiting_for_new_date)

@dp.message(GameEditState.waiting_for_new_date)
async def edit_game_date_finish(message: types.Message, state: FSMContext):
    await message.delete()
    await delete_prev_bot_msg(state)
    data = await state.get_data()
    gid = data['edit_game_id']
    if len(message.text) < 8:
        msg = await message.answer("❌ Неверный формат! Попробуйте снова:", reply_markup=get_back_to_view_kb("view_game", gid))
        await state.update_data(last_bot_msg_id=msg.message_id)
        return
    await update_game_field(gid, 'game_date', message.text)
    await return_to_game_view(message, gid, state)

@dp.callback_query(F.data.startswith("edit_game_map_"))
async def edit_game_map_start(callback: types.CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id): return
    gid = int(callback.data.split("_")[-1])
    await state.update_data(edit_game_id=gid)
    
    msg = await callback.message.edit_text(
        "✏️ Выберите *карту*:", 
        reply_markup=get_map_select_kb(mode="edit", game_id=gid),
        parse_mode="MarkdownV2"
    )
    await state.update_data(last_bot_msg_id=msg.message_id, chat_id=callback.message.chat.id)
    await state.set_state(GameEditState.waiting_for_new_map)

# ОБРАБОТЧИК КНОПКИ КАРТЫ ПРИ РЕДАКТИРОВАНИИ
@dp.callback_query(F.data.startswith("set_edit_map_"))
async def process_edit_map_btn(callback: types.CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id): return
    # set_edit_map_{gid}_{map_name}
    parts = callback.data.split("_")
    gid = int(parts[3])
    map_name = parts[4]
    
    await update_game_field(gid, 'map_name', map_name)
    await callback.answer("Карта обновлена")
    await return_to_game_view(callback.message, gid, state)

# Оставляем возможность ввести вручную, если состояние активно
@dp.message(GameEditState.waiting_for_new_map)
async def edit_game_map_finish_text(message: types.Message, state: FSMContext):
    await message.delete()
    await delete_prev_bot_msg(state)
    data = await state.get_data()
    gid = data['edit_game_id']
    await update_game_field(gid, 'map_name', message.text)
    await return_to_game_view(message, gid, state)

@dp.callback_query(F.data.startswith("edit_game_score_"))
async def edit_game_score_start(callback: types.CallbackQuery, state: FSMContext):
    if not await check_is_admin(callback.from_user.id): return
    gid = int(callback.data.split("_")[-1])
    await state.update_data(edit_game_id=gid)
    msg = await callback.message.edit_text("✏️ Введите новый *счет* \\(например `13-11`\\):", reply_markup=get_back_to_view_kb("view_game", gid), parse_mode="MarkdownV2")
    await state.update_data(last_bot_msg_id=msg.message_id, chat_id=callback.message.chat.id)
    await state.set_state(GameEditState.waiting_for_new_score)

@dp.message(GameEditState.waiting_for_new_score)
async def edit_game_score_finish(message: types.Message, state: FSMContext):
    await message.delete()
    await delete_prev_bot_msg(state)
    data = await state.get_data()
    gid = data['edit_game_id']
    try:
        s1, s2 = map(int, message.text.split('-'))
        await update_game_field(gid, 'score_t1', s1)
        await update_game_field(gid, 'score_t2', s2)
        await update_game_field(gid, 'total_rounds', s1 + s2)
        await return_to_game_view(message, gid, state)
    except:
        msg = await message.answer("❌ Ошибка! Формат `13-11`", reply_markup=get_back_to_view_kb("view_game", gid), parse_mode="MarkdownV2")
        await state.update_data(last_bot_msg_id=msg.message_id)

async def return_to_game_view(message, game_id, state):
    await state.clear()
    game = await get_game_by_id(game_id)
    if not game: return

    from database import get_tournament_by_id
    tour = await get_tournament_by_id(game['tournament_id'])
    season_name = tour['season'] if tour else ""
    
    text = format_game_stats(game, season_name)
    kb_rows = []
    if await check_is_admin(message.from_user.id):
        kb_rows.append([
            InlineKeyboardButton(text="✏️ Дату", callback_data=f"edit_game_date_{game_id}"),
            InlineKeyboardButton(text="✏️ Карту", callback_data=f"edit_game_map_{game_id}"),
            InlineKeyboardButton(text="✏️ Счет", callback_data=f"edit_game_score_{game_id}")
        ])
        kb_rows.append([InlineKeyboardButton(text="❌ УДАЛИТЬ ИГРУ", callback_data=f"del_game_confirm_{game_id}")])
    kb_rows.append([InlineKeyboardButton(text="🔙 К списку игр", callback_data=f"list_games_{game['tournament_id']}")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="MarkdownV2")

# --- СПИСОК ИГР (ПРОСМОТР) ---

@dp.callback_query(F.data.startswith("list_games_"))
async def start_games_list(callback: types.CallbackQuery, state: FSMContext):
    tid = int(callback.data.split("_")[-1])
    await state.update_data(current_tour_id=tid, date_filter=None)
    await show_games_page(callback, 0, state)

@dp.callback_query(F.data.startswith("game_page_"))
async def games_pagination(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    page = int(parts[-1])
    await show_games_page(callback, page, state)

@dp.callback_query(F.data.startswith("filter_games_date_"))
async def games_filter_date_ask(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("📅 Введите дату для фильтрации в формате `YYYY.MM.DD` (например `2024.01.25`):", parse_mode="MarkdownV2")
    await state.set_state(GameListState.filter_date)

@dp.message(GameListState.filter_date)
async def games_filter_date_apply(message: types.Message, state: FSMContext):
    await message.delete() 
    date_val = message.text.strip()
    await state.update_data(date_filter=date_val)
    fake_cb = types.CallbackQuery(id='0', from_user=message.from_user, chat_instance='0', message=message, data='fake')
    await show_games_page(fake_cb, 0, state)

async def show_games_page(callback: types.CallbackQuery, page, state: FSMContext):
    data = await state.get_data()
    tid = data.get('current_tour_id')
    date_filter = data.get('date_filter')
    
    games, pages, count = await get_games_paginated(tid, page, 5, date_filter)
    
    filter_txt = f"\n📅 Фильтр: `{escape_md(date_filter)}`" if date_filter else ""
    text = f"📜 *Список игр* турнира \\#{tid}\nВсего: {count}{filter_txt}"
    
    kb = get_games_carousel_kb(games, page, pages, tid)
    
    try: await callback.message.delete()
    except: pass
    await callback.message.answer(text, reply_markup=kb, parse_mode="MarkdownV2")

async def main():
    await init_db()
    print("🚀 Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: print("Стоп.")