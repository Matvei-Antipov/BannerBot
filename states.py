from aiogram.fsm.state import State, StatesGroup
from aiogram.filters.callback_data import CallbackData

# --- АДМИНКА ---
class AdminAddAdmin(StatesGroup):
    waiting_for_username = State()

class AdminAddOwner(StatesGroup):
    waiting_for_username = State()

# --- КОМАНДЫ ---
class AdminTeamCreate(StatesGroup):
    waiting_for_name = State()
    waiting_for_tag = State()
    waiting_for_roster = State()
    waiting_for_logo = State()

class AdminTeamEdit(StatesGroup):
    waiting_for_new_value = State()

# --- ТУРНИРЫ ---
class TournamentCreate(StatesGroup):
    waiting_for_tour_name = State()
    waiting_for_tour_season = State()
    waiting_for_year = State()
    waiting_for_qualifiers = State()
    waiting_for_group_stage = State()
    waiting_for_logo = State()
    waiting_for_prize_currency = State()
    waiting_for_prize_total = State()
    waiting_for_prize_distribution = State()
    waiting_for_mvp_decision = State()
    waiting_for_mvp_amount = State()

class AdminTourEdit(StatesGroup):
    waiting_for_new_value = State()

class TourAddTeam(StatesGroup):
    waiting_for_tag = State()

class TourSetWinner(StatesGroup):
    selecting_place = State() # 1, 2, 3
    selecting_team = State()

# --- РЕГИСТРАЦИЯ ИГРЫ ---
class GameRegister(StatesGroup):
    selecting_tournament = State()
    waiting_for_format = State()    
    waiting_for_date = State()      
    waiting_for_map = State()       
    waiting_for_score = State()     
    waiting_for_team1_tag = State() 
    waiting_for_player_stats = State() 
    waiting_for_team2_tag = State() 

# --- РЕДАКТИРОВАНИЕ ИГРЫ ---
class GameEditState(StatesGroup):
    waiting_for_new_date = State()
    waiting_for_new_map = State()
    waiting_for_new_score = State()

# --- УПРАВЛЕНИЕ ИГРОКОМ ---
class PlayerAdminState(StatesGroup):
    waiting_for_new_name = State() 
    waiting_for_new_nick = State() 
    selecting_transfer_team = State()

# --- СПИСКИ ИГР ---
class GameListState(StatesGroup):
    selecting_tournament_for_list = State()
    viewing = State()
    filter_date = State() 

# --- СПИСКИ КОМАНД ---
class TeamListState(StatesGroup):
    viewing = State()

# --- КАЛБЕКИ ---
class TournamentNav(CallbackData, prefix="turn_nav"):
    action: str
    index: int
    id: int
