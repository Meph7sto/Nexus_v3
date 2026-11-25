package service

func ProvideMonthlyLedgerService(repo MonthlyLedgerRepository, email *NotificationEmailService, settings SettingRepository, tasks *SharedTaskGate) *MonthlyLedgerService {
	s := NewMonthlyLedgerService(repo)
	s.emailSettings = settings
	s.emailSend = email.Send
	tasks.registerWorker(&s.emailWorker, s.runEmailNotifications)
	tasks.registerWorker(&s.incomeWorker, s.runIncomeSchedules)
	return s
}
