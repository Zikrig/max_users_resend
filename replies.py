"""
Тексты ответов бота (реплики для пользователей MAX).
"""

from __future__ import annotations

# --- UI: эмодзи и подписи кнопок ---
DELEGATED_CHANNEL_EMOJI = "⤴ "

BTN_CHANNELS = "Каналы"
BTN_DELEGATES = "Делегаты"
BTN_AD_LINK = "Рекламная ссылка"
BTN_POST_BUTTONS = "Кнопки в посте"
BTN_STATS = "Статистика"
BTN_MASTER_ADMINS = "Мастер-админы (конфиг)"
BTN_BACK = "Назад"
BTN_ADD_CHANNEL = "Добавить канал"
BTN_ADD_DELEGATE = "Добавить делегата"
BTN_ADD_MASTER = "Добавить мастера"
BTN_CHANGE_TEXT = "Изменить текст"
BTN_CHANGE_LINK = "Изменить ссылку"
BTN_CHAT_ENTRY_TEXT = "Текст: вход в чат"
BTN_MSG_LINK_TEXT = "Текст: к сообщению"
BTN_MUTE = "Mute"
BTN_POSTS = "Посты"
BTN_DELETE = "Удалить"
BTN_EDIT_RANGE = "Изменить диапазон"
BTN_CHANGE_POST_TEXT = "Поменять текст"
BTN_CHANGE_POST_IMAGE = "Поменять картинку"
BTN_MUTE_ON = "Выключить Mute"
BTN_MUTE_OFF = "Включить Mute"
NAV_PREV = "←"
NAV_NEXT = "→"

# --- Доступ и команды ---
GUEST_NO_ACCESS = (
    "Этот бот только для администраторов аккаунтов и мастер-админов. "
    "Обратитесь к владельцу или отправьте /start после того, как вас добавят."
)

ACCESS_DENIED = (
    "Доступ к боту есть только у мастер-админов и участников аккаунтов. "
    "Попросите владельца добавить вас делегатом."
)

MASTER_ONLY_ADMIN_CMD = "Команда /admin только для мастер-админов."
MASTER_ONLY_STATS_CMD = "Команда /stats только для мастер-админов."


def stats_line_callback_total(n: int) -> str:
    return f"Переходов по рекламной кнопке (учёт callback): {n}"


def stats_line_short(n: int) -> str:
    return f"Переходов по рекламной кнопке (callback): {n}"


# --- Меню (заголовки) ---
USER_MENU_INTRO = (
    "Каналы и делегаты вашего аккаунта.\n"
    "Глобальная реклама и мастер-настройки — команда /admin (только мастера)."
)

MASTER_MENU_INTRO = (
    "Мастер-панель: глобальная реклама, кнопки под постами, счётчик кликов по рекламе."
)

# --- FSM / сессии ---
SESSION_MASTER_RESET = "Сессия ввода сброшена (нужны права мастера)."
ONLY_ENV_ADDS_MASTERS = "Только мастер из .env может добавлять мастеров в конфиг."

# --- Мастер: реклама и кнопки ---
AD_TEXT_CHANGED = "Текст рекламы изменен: {text}"
AD_LINK_INVALID = "Ссылка должна начинаться с http:// или https://"
AD_LINK_CHANGED = "Ссылка рекламы изменена"
CHAT_BTN_TEXT_CHANGED = "Текст кнопки чата изменен: {text}"
MSG_BTN_TEXT_CHANGED = "Текст кнопки к сообщению изменен: {text}"

PROMOTED_NEED_NUMERIC = "Нужен числовой user_id."
PROMOTED_ALREADY_ENV = "Уже в .env."
PROMOTED_ALREADY_LIST = "Уже в списке."
PROMOTED_ADDED = "Мастер добавлен в конфиг: {mid}"
PROMOTED_REMOVED = "Удалён из мастеров конфига: {mid}"

PROMPT_NEW_MASTER_ID = "Введите user_id нового мастера (в конфиге):"
PROMPT_AD_TEXT = "Введите новый текст рекламной кнопки:"
PROMPT_AD_URL = "Введите новую ссылку рекламы:"
PROMPT_CHAT_BTN = "Введите новый текст кнопки входа в чат комментариев:"
PROMPT_MSG_BTN = (
    "Введите новый текст кнопки, которая ведёт к конкретному сообщению в чате комментариев:"
)

# --- Привязка каналов ---
ERR_RESOLVE_CHANNEL_DEFAULT = "Не удалось определить канал."
ERR_BOT_MEMBERSHIP_DEFAULT = "Ошибка проверки прав бота."
CHANNEL_NEED_BOT_ADMIN_EDIT = (
    "Бот должен быть администратором канала с правом редактировать сообщения "
    "(или владельцем). Если доступы уже выданы — см. лог members/me на сервере."
)
CHANNEL_ALREADY_BOUND = (
    "Этот канал уже подключён. Удалите запись в меню «Каналы» перед повторной привязкой."
)
CHANNEL_STEP_COMMENTS = (
    "Канал принят. Теперь отправьте ссылку-приглашение в чат комментариев "
    "(или числовой chat_id чата). Бот должен быть администратором с правом писать в чат."
)
BIND_SESSION_RESET = "Сессия добавления канала сброшена. Начните снова из меню «Каналы»."
ERR_RESOLVE_CHAT_DEFAULT = "Не удалось определить чат."
COMMENTS_SAME_AS_CHANNEL = "Чат комментариев не должен совпадать с каналом. Укажите другой чат."
COMMENTS_NEED_BOT_ADMIN = (
    "Бот должен быть администратором чата с правом писать сообщения "
    "(или владельцем). Если доступы уже выданы — см. лог members/me на сервере."
)
INVITE_SAVE_FAILED = (
    "Не удалось сохранить ссылку-приглашение: пришлите полную https-ссылку из приглашения в чат "
    "(или chat_id, если у чата есть публичная ссылка в данных API)."
)
CHANNEL_PAIR_CONNECTED = "Канал и чат комментариев подключены."

# --- Делегаты ---
DELEGATE_NEED_NUMERIC = "Нужно отправить только числовой user_id нового админа."
DELEGATE_NO_MASTER = "Нельзя добавить мастер-админа как делегата."
DELEGATE_ALREADY_IN_TREE = "Этот пользователь уже в дереве делегатов (только один «спонсор»)."
DELEGATE_HAS_OWN_ACCOUNT = "У этого пользователя уже есть свои каналы (отдельный аккаунт)."
DELEGATE_ADDED = "Делегат добавлен: {uid}"
DELEGATE_REMOVED = "Делегат удалён: {uid}"
DELEGATE_REMOVE_NOT_YOURS = "Можно удалять только своих прямых делегатов."
PROMPT_DELEGATE_ID = "Введите user_id нового админа:"

DELEGATES_MENU = (
    "Делегаты (видят только каналы, которые добавили вы, и наследованные от вас по правилам бота).\n"
    "Текущие: {list_text}"
)

# --- Каналы / посты / mute ---
NO_ACCESS_CHANNEL = "Нет доступа к этому каналу."
CHANNEL_NOT_FOUND_OR_NO_ACCESS = "Канал не найден или нет доступа."
MUTE_RANGE_FORMAT = "Формат: HH:MM-HH:MM, например 12:00-14:00 или 21:33-07:00"
BINDING_NOT_FOUND = "Привязка канала не найдена."
MUTE_RANGE_UPDATED = "Диапазон Mute обновлен: {qh} (МСК)"

POST_NOT_FOUND = "Пост не найден или срок хранения истёк."
POST_EDIT_NO_IMAGE = (
    "В сообщении нет вложения с картинкой. Отправьте фото или изображение одним сообщением."
)
IMAGE_UPDATED_BOTH = "Картинка обновлена в канале и в копии в чате комментариев."
IMAGE_UPDATED_CHANNEL = "Картинка в канале обновлена."
IMAGE_UPDATE_FAILED = "Не удалось обновить вложения (проверьте права бота и формат файла)."
TEXT_UPDATED_BOTH = "Текст обновлён в канале и в копии в чате комментариев."
TEXT_UPDATED_CHANNEL = "Текст поста в канале обновлён."
POST_EDIT_FAILED = "Не удалось изменить пост (проверьте права бота и message_id)."
PROMPT_POST_NEW_TEXT = "Введите новый текст поста в канале (одним сообщением):"
PROMPT_POST_NEW_IMAGE = (
    "Отправьте одно сообщение с новой картинкой (фото или файл изображения). Текст поста не меняется."
)

BIND_CHANNEL_PROMPT = (
    "Отправьте ссылку-приглашение в канал или числовой chat_id канала.\n"
    "Бот уже должен быть в канале администратором с правом редактировать сообщения."
)

LIST_EMPTY_DASH = "—"


def channel_title_fallback(channel_id: int) -> str:
    return f"Канал {channel_id}"


POSTS_EMPTY = (
    "Постов с кнопками для «{title}» пока нет (бот ещё не обрабатывал посты или записи старше 3 суток удалены)."
)


def posts_list_caption(title: str, page: int, max_page: int, total: int) -> str:
    return (
        f"Посты канала: {title}\n"
        f"Новые сверху. Страница {page + 1} из {max_page + 1}. Всего: {total}. Хранение до 3 суток."
    )


POST_DETAIL_PREFIX = "Текст поста:\n\n"
EMPTY_POST_PLACEHOLDER = "(пустой текст)"

CHANNELS_HEADER = "Подключённые каналы (канал → чат комментариев)."
CHANNELS_DELEGATED_HINT = "{emoji} — канал добавлен не вами (наследован от вышестоящего админа)."
CHANNELS_EMPTY = "Пока ничего не подключено — нажмите «Добавить канал»."


def channel_list_line(n: int, ct: str, cid: int, cct: str, ccid: int) -> str:
    return f"{n}. {ct} ({cid}) → {cct} ({ccid})"


def channel_detail_text(ct: str, cid: int, cct: str, ccid: int) -> str:
    return (
        f"Канал: {ct}\n"
        f"channel_id: {cid}\n\n"
        f"Чат комментариев: {cct}\n"
        f"comments_chat_id: {ccid}"
    )


def mute_submenu_text(title: str, mute_en: bool, current: str) -> str:
    st = "включен" if mute_en else "выключен"
    return (
        f"Mute (чат комментариев к этому каналу)\n"
        f"{title}\n"
        f"Статус: {st}\n"
        f"Диапазон: {current}\n"
        "Часовой пояс: Europe/Moscow (МСК)"
    )


MUTE_RANGE_NOT_SET = "не настроены"

MASTER_AD_SUBMENU = "Реклама\nТекст: {ad_text}\nСсылка: {ad_url}"

MASTER_BTNS_SUBMENU = (
    "Кнопки под постом в канале (одинаковые для всех подключённых каналов).\n"
    "Ссылку-приглашение в чат комментариев для каждого канала задаёте в пользовательском меню «Каналы».\n\n"
    "Текст кнопки входа в чат: {chat_btn}\n"
    "Текст кнопки к сообщению: {msg_btn}"
)

MASTER_LIST_HEADER = "Мастер-админы (хранятся в config.json, не в .env)."
MASTER_LIST_LINE = "Список: {items}"


def master_list_line(items_joined: str) -> str:
    return MASTER_LIST_LINE.format(items=items_joined or LIST_EMPTY_DASH)

ERR_BAD_POST_REF = "Некорректная ссылка на пост."
ERR_BAD_REF = "Некорректная ссылка."
ERR_CHANNEL_MISMATCH = "Несовпадение канала."
ERR_BAD_CHANNEL_ID = "Некорректный id канала."
ERR_BAD_USER_ID = "Некорректный user_id для удаления."

BINDING_REMOVE_NONE = "Такой привязки не найдено."
BINDING_REMOVED = "Привязка канала удалена."

PROMPT_MUTE_RANGE = "Введите диапазон, например 12:00-14:00 или 21:33-07:00"

MUTE_TOGGLED = "Mute для этого канала {state}"


def mute_state_word(on: bool) -> str:
    return "включен" if on else "выключен"


# --- Разрешение чатов (resolve_chat_from_input / find_chat_by_invite_url) ---
EMPTY_INPUT = "Пустой ввод."


def chat_not_found_by_id(chat_id: int) -> str:
    return f"Чат с id={chat_id} не найден или бот не состоит в нём."


def chat_list_fetch_error(exc: str) -> str:
    return f"Не удалось получить список чатов: {exc}"


CHAT_NOT_IN_BOT_LIST = (
    "Чат не найден среди чатов бота. Добавьте бота в канал/чат по этой ссылке, "
    "затем повторите ввод."
)


def membership_http_error(code: int) -> str:
    return f"Не удалось проверить права бота (HTTP {code})."


MEMBERSHIP_BAD_RESPONSE = "Некорректный ответ API при проверке прав."

# --- HTTP webhook (GET) ---
WEBHOOK_GET_DETAIL = (
    "События от MAX приходят POST с телом Update. Откройте в браузере только для проверки; "
    "бот отвечает в чатах MAX, не здесь."
)
