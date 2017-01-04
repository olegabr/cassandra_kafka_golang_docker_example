package common

// Тип для перечисления состояний пользователя
type UserState int

// Состояния, в которых может быть пользователь
const (
	UserStateNew             UserState = 1
	UserStateEmailVerified   UserState = 2
	UserStatePhoneVerified   UserState = 4
	UserStateBanned          UserState = 8
	UserStateRemoved         UserState = 16
)
