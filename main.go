package main

import (
	"database/sql"
	"fmt"
	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
	"github.com/liushuochen/gotable"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
	"os"
	"time"
)

var srcDb *sql.DB
var log = logrus.New()

type DbConnStr struct {
	SrcHost       string
	SrcUserName   string
	SrcPassword   string
	SrcDatabase   string
	SrcPort       int
	delaySeconds  int
	maxRunSeconds int
}

func getConfig() (connStr *DbConnStr) {
	viper.SetConfigFile("./dbcfg.yml")
	// 通过viper读取配置文件进行加载
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal(viper.ConfigFileUsed(), " has some error please check your yml file ! ", "Detail-> ", err)
	}
	connStr = new(DbConnStr)
	connStr.SrcHost = viper.GetString("src.host")
	connStr.SrcUserName = viper.GetString("src.username")
	connStr.SrcPassword = viper.GetString("src.password")
	connStr.SrcDatabase = viper.GetString("src.database")
	connStr.SrcPort = viper.GetInt("src.port")
	connStr.delaySeconds = viper.GetInt("delaySeconds")
	connStr.maxRunSeconds = viper.GetInt("maxRunSeconds")
	return connStr
}

func PrepareSrc(connStr *DbConnStr) {
	// 生成目标连接
	Host := connStr.SrcHost
	UserName := connStr.SrcUserName
	Password := connStr.SrcPassword
	Database := connStr.SrcDatabase
	Port := connStr.SrcPort
	destConn := fmt.Sprintf("%s:%s@tcp(%s:%v)/%s?charset=utf8&maxAllowedPacket=0", UserName, Password, Host, Port, Database)
	var err error
	srcDb, err = sql.Open("mysql", destConn)
	if err != nil {
		log.Fatal("please check MySQL yml file ", err)
	}
	c := srcDb.Ping()
	if c != nil {
		log.Fatal("connect target MySQL failed ", c)
	}
	srcDb.SetConnMaxLifetime(2 * time.Hour) // 一个连接被使用的最长时间，过一段时间之后会被强制回收
	srcDb.SetMaxIdleConns(0)                // 最大空闲连接数，0为不限制
	srcDb.SetMaxOpenConns(0)                // 设置连接池最大连接数
}

func innodbLockInfo() {
	connStr := getConfig()
	// 生成源库数据库连接
	PrepareSrc(connStr)
	var (
		dbTime               string
		waitingId            string
		blockingId           string
		duration             string
		state                string
		waitingQuery         string
		blockingCurrentQuery string
		blockingQueryHistory string
		blkingDb             string
		blockingHost         string
	)
	table, err := gotable.Create("blockingDbName", "dbTime", "waitingId", "blockingId", "duration", "state", "waitingQuery", "blockingCurrentQuery", "blockingQueryHistory", "blockingHost")
	if err != nil {
		fmt.Println("Create table failed: ", err.Error())
		return
	}
	sqlStr := "SELECT\n\tnow(),\n\tr.trx_mysql_thread_id waiting_id,\n\tb.trx_mysql_thread_id blocking_id,\n\tconcat( timestampdiff( SECOND, r.trx_wait_started, CURRENT_TIMESTAMP ()), 's' ) AS duration,\n\tt.processlist_command state,\n\tr.trx_query waiting_query,\n\tIFNULL(b.trx_query,'No sql running') blocking_current_query,\n\tgroup_concat( LEFT ( h.sql_text, 10000 ) ORDER BY h.TIMER_START DESC SEPARATOR ';' ) AS blocking_thd_query_history FROM\n\tinformation_schema.innodb_lock_waits w\n\tJOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id\n\tJOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id\n\tLEFT JOIN PERFORMANCE_SCHEMA.threads t ON t.processlist_id = b.trx_mysql_thread_id\n\tLEFT JOIN PERFORMANCE_SCHEMA.events_statements_history h USING ( thread_id ) \nGROUP BY\n\tthread_id,\n\tr.trx_id \nORDER BY\n\tr.trx_wait_started;"
	rows, err := srcDb.Query(sqlStr)
	if err != nil {
		log.Error(err)
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&dbTime, &waitingId, &blockingId, &duration, &state, &waitingQuery, &blockingCurrentQuery, &blockingQueryHistory)
		if err != nil {
			log.Error(err)
		}
		err := srcDb.QueryRow(fmt.Sprintf("select db,host from information_schema.PROCESSLIST where id=%s limit 1", blockingId)).Scan(&blkingDb, &blockingHost)
		if err != nil {
			log.Error(err)
		}
		tabRet := []string{blkingDb, dbTime, waitingId, blockingId, duration, state, waitingQuery, blockingCurrentQuery, blockingQueryHistory, blockingHost}
		_ = table.AddRow(tabRet)
	}
	defer srcDb.Close()
	table.Align("dbName", 1)
	table.Align("dbTime", 1)
	table.Align("waitingId", 1)
	table.Align("blockingId", 1)
	table.Align("duration", 1)
	table.Align("state", 1)
	table.Align("waitingQuery", 1)
	table.Align("blockingCurrentQuery", 1)
	table.Align("blockingQueryHistory", 1)
	table.Align("blockingHost", 1)
	if len(waitingId) > 0 {
		// 打开外部文本文件用于转储信息
		file, err := os.OpenFile("log.txt", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
		if err != nil {
			fmt.Println("Can not create log.txt", err)
			return
		}
		defer file.Close()
		// 设置输出到终端和文件
		multiWriter := io.MultiWriter(os.Stdout, file)
		fmt.Println("ROW LOCK INFO:")
		fmt.Fprintln(multiWriter, table) // 同时打印到终端以及转储到平面文件
	} else {
		log.Info(time.Now().Format("2006-01-02 15:04:05"), " [No row lock info]")
	}
}

func tableLockInfo() {
	connStr := getConfig()
	// 生成源库数据库连接
	PrepareSrc(connStr)
	var (
		connection  string
		command     string
		duration    string
		state       string
		info        string
		trx_started string
	)
	table, err := gotable.Create("connection", "command", "duration", "state", "tableLockInfo", "trx_started")
	if err != nil {
		fmt.Println("Create table failed: ", err.Error())
		return
	}
	sqlStr := "SELECT concat(user,'@',host,':',db) As connection,command,time,state,ifnull(info,'no sql'),ifnull(trx_started,'null') FROM INFORMATION_SCHEMA.processlist p left join INFORMATION_SCHEMA.INNODB_TRX trx on p.id = trx.trx_mysql_thread_id\nWHERE (TO_SECONDS(now())-TO_SECONDS(trx_started) >= (SELECT MAX(Time) FROM INFORMATION_SCHEMA.processlist\nWHERE STATE like 'Waiting for%' and command != 'Daemon') or STATE like 'Waiting for%') and command != 'Daemon'\norder by trx_started desc,time desc;"
	rows, err := srcDb.Query(sqlStr)
	if err != nil {
		log.Error(err)
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&connection, &command, &duration, &state, &info, &trx_started)
		if err != nil {
			log.Error(err)
		}
		tabRet := []string{connection, command, duration, state, info, trx_started}
		_ = table.AddRow(tabRet)
	}
	defer srcDb.Close()
	table.Align("connection", 1)
	table.Align("command", 1)
	table.Align("duration", 1)
	table.Align("state", 1)
	table.Align("tableLockInfo", 1)
	table.Align("trx_started", 1)
	if len(command) > 0 {
		// 打开外部文本文件用于转储信息
		file, err := os.OpenFile("log.txt", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
		if err != nil {
			fmt.Println("Can not create log.txt", err)
			return
		}
		defer file.Close()
		// 设置输出到终端和文件
		multiWriter := io.MultiWriter(os.Stdout, file)
		fmt.Println("TABLE LOCK INFO:")
		fmt.Fprintln(multiWriter, table) // 同时打印到终端以及转储到平面文件
	}
}

// 一个无缓冲泳道
var stopFlag = make(chan bool)

// CloseDemoScheduler 对外提供一个往通道写消息的函数，如果想关闭定时任务，调用该函数即可。
func CloseDemoScheduler() {
	stopFlag <- false
}

// InitDemoScheduler 初始化 demo 定时器
func InitDemoScheduler(delaySeconds int) {
	// 每 n 秒钟时执行一次
	ticker := time.NewTicker(time.Duration(delaySeconds) * time.Second) // 创建一个定时器
	go func() {                                                         // 用新协程去执行定时任务
		defer func() {
			if r := recover(); r != nil {
				log.Error("定时器发生错误，%v", r)
			}
			ticker.Stop() // 意外退出时关闭定时器
		}()
		innodbLockInfo() // 协程启动时启动一次，之后每 5 秒执行一次，如果没有这行，只有等到协程启动后的第 5 秒才会第一次执行任务
		for {            // 用上一个死循环，不停地执行，否则只会执行一次
			select {
			case <-ticker.C: // 时间到了就会触发这个分支的执行，其实时间到了定时器会往ticker.C 这个 channel 中写一条数据，随后被 select 捕捉到channel中有数据可读，就读取channel数据，执行相应分支的语句
				innodbLockInfo()
			case <-stopFlag: // 定时任务进程在监听定时器的同时也监听这个无缓冲泳道，如果监听到无缓冲泳道的消息，则立刻 return 终止协程，也就终止了定时任务。
				return
			}
		}
	}()
	go func() { // 用新协程去执行定时任务
		defer func() {
			if r := recover(); r != nil {
				log.Error("定时器发生错误，%v", r)
			}
			ticker.Stop() // 意外退出时关闭定时器
		}()
		tableLockInfo() // 协程启动时启动一次，之后每 5 秒执行一次，如果没有这行，只有等到协程启动后的第 5 秒才会第一次执行任务
		for {           // 用上一个死循环，不停地执行，否则只会执行一次
			select {
			case <-ticker.C: // 时间到了就会触发这个分支的执行，其实时间到了定时器会往ticker.C 这个 channel 中写一条数据，随后被 select 捕捉到channel中有数据可读，就读取channel数据，执行相应分支的语句
				tableLockInfo()
			case <-stopFlag: // 定时任务进程在监听定时器的同时也监听这个无缓冲泳道，如果监听到无缓冲泳道的消息，则立刻 return 终止协程，也就终止了定时任务。
				return
			}
		}
	}()
}

func Info(ver string) {
	color.Red("DDDDDDDDDDDDD      BBBBBBBBBBBBBBBBB               AAA                  GGGGGGGGGGGGG     OOOOOOOOO     DDDDDDDDDDDDD        ")
	color.Red("D::::::::::::DDD   B::::::::::::::::B             A:::A              GGG::::::::::::G   OO:::::::::OO   D::::::::::::DDD     ")
	color.Red("D:::::::::::::::DD B::::::BBBBBB:::::B           A:::::A           GG:::::::::::::::G OO:::::::::::::OO D:::::::::::::::DD   ")
	color.Red("DDD:::::DDDDD:::::DBB:::::B     B:::::B         A:::::::A         G:::::GGGGGGGG::::GO:::::::OOO:::::::ODDD:::::DDDDD:::::D  ")
	color.Red("  D:::::D    D:::::D B::::B     B:::::B        A:::::::::A       G:::::G       GGGGGGO::::::O   O::::::O  D:::::D    D:::::D ")
	color.Red("  D:::::D     D:::::DB::::B     B:::::B       A:::::A:::::A     G:::::G              O:::::O     O:::::O  D:::::D     D:::::D")
	color.Red("  D:::::D     D:::::DB::::BBBBBB:::::B       A:::::A A:::::A    G:::::G              O:::::O     O:::::O  D:::::D     D:::::D")
	color.Red("  D:::::D     D:::::DB:::::::::::::BB       A:::::A   A:::::A   G:::::G    GGGGGGGGGGO:::::O     O:::::O  D:::::D     D:::::D")
	color.Red("  D:::::D     D:::::DB::::BBBBBB:::::B     A:::::A     A:::::A  G:::::G    G::::::::GO:::::O     O:::::O  D:::::D     D:::::D")
	color.Red("  D:::::D     D:::::DB::::B     B:::::B   A:::::AAAAAAAAA:::::A G:::::G    GGGGG::::GO:::::O     O:::::O  D:::::D     D:::::D")
	color.Red("  D:::::D     D:::::DB::::B     B:::::B  A:::::::::::::::::::::AG:::::G        G::::GO:::::O     O:::::O  D:::::D     D:::::D")
	color.Red("  D:::::D    D:::::D B::::B     B:::::B A:::::AAAAAAAAAAAAA:::::AG:::::G       G::::GO::::::O   O::::::O  D:::::D    D:::::D ")
	color.Red("DDD:::::DDDDD:::::DBB:::::BBBBBB::::::BA:::::A             A:::::AG:::::GGGGGGGG::::GO:::::::OOO:::::::ODDD:::::DDDDD:::::D  ")
	color.Red("D:::::::::::::::DD B:::::::::::::::::BA:::::A               A:::::AGG:::::::::::::::G OO:::::::::::::OO D:::::::::::::::DD   ")
	color.Red("D::::::::::::DDD   B::::::::::::::::BA:::::A                 A:::::A GGG::::::GGG:::G   OO:::::::::OO   D::::::::::::DDD     ")
	color.Red("DDDDDDDDDDDDD      BBBBBBBBBBBBBBBBBAAAAAAA                   AAAAAAA   GGGGGG   GGGG     OOOOOOOOO     DDDDDDDDDDDDD        ")
	colorStr := color.New()
	colorStr.Add(color.FgHiGreen)
	colorStr.Printf("innodbLockWaitCheck\n")
	colorStr.Printf("Powered By: DBA Team Of Infrastructure Research Center \nRelease version v" + ver)
	//time.Sleep(5 * 100 * time.Millisecond)
	fmt.Printf("\n")
}

func main() {
	log.SetReportCaller(true)
	Info("0.0.2")
	configStr := getConfig()
	// 初始化定时器，每 5s 会打印一个「demo........」
	InitDemoScheduler(configStr.delaySeconds)
	// 等待，避免主线程退出，实际应用时这里可以时启动 http 服务器的监听动作，或者启动 rpc 服务的监听动作，所以不需要 sleep
	time.Sleep(time.Duration(configStr.maxRunSeconds) * time.Second)
}
