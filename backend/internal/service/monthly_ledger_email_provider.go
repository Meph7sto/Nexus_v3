package service

func ProvideMonthlyLedgerService(repo MonthlyLedgerRepository, email *NotificationEmailService, settings SettingRepository, tasks *SharedTaskGate) *MonthlyLedgerService {
	s := NewMonthlyLedgerService(repo)
	s.emailSettings = settings
	s.emailSend = email.Send
	tasks.Start(func() {
		s.StartEmailNotifications(email)
		s.StartIncomeSchedules()
	})
	return s
}
