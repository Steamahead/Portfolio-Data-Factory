' run_hidden.vbs - uruchamia daily scraper bat BEZ widocznego okna konsoli.
' Uzywany jako akcja zadania w Harmonogramie, zeby ~1s sprawdzenie bramki
' (i wieczorny realny run) nie wyskakiwaly oknem CMD przy kazdym odblokowaniu
' ekranu. Tryb okna 0 = ukryte; bWaitOnReturn = True, zeby zadanie pozostalo
' "running" az bat sie skonczy (zachowuje MultipleInstancesPolicy=IgnoreNew,
' czyli brak nakladajacych sie runow). Exit code bata jest propagowany jako
' wynik zadania (przydatne do diagnostyki).
Dim sh, bat
bat = "C:\Users\sadza\PycharmProjects\portfolio-data-factory\run_daily_scrapers.bat"
Set sh = CreateObject("WScript.Shell")
WScript.Quit(sh.Run("""" & bat & """", 0, True))
