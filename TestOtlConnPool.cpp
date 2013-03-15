#include "database/dbpool.h"
using namespace OTL;
#include <string>

// 测试非单件/单件连接池
static int test_pool();
static int test_singleton_pool();
static int test_convert_datetime();

int main(int argc, char** argv)
{
    // 测试日期时间的转换
    test_convert_datetime();

    // 测试非单件连接池
    test_pool();

    // 测试单件连接池
    test_singleton_pool();

    return 0;
}

// 测试非单件连接池
int test_pool()
{
    std::string strErrMsg;
    OTL::CDBConnPool dbpool;
    int nConnNum = 2;
    if( nConnNum != dbpool.Init("new_elevator/1234@Elevator_8", nConnNum) )
    {
        printf("Initialized database connection pool failed, reason: %s.\n", dbpool.GetLastError());
        return -1;
    }
    for(int i = 0; i < 5; ++i)
    {
        // 获取当前数据库的时间
        OTL::CDBAppConn conn(&dbpool);
        if( !conn.Good() )
        {
            printf("Get connection failed, reason: %s.\n", conn.GetLastError());
            continue;
        }

        try
        {   
            otl_stream ostr(1, "select to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') from dual", conn);
            char date[32] = { 0 };
            ostr >> date;
            printf("Get current date is : %s.", date);
        }
        catch( otl_exception & e )
        {
            strErrMsg = conn.GetErrFromException(e);
            printf(strErrMsg.c_str());
        }
        getchar();
    }
    return 0;
}


// 自定义一个单件连接池
SINGLETON_DBCONNPOOL_DECLARE( TestDBPool );
SINGLETON_DBCONNPOOL_DEFINE( TestDBPool );

// 测试单件连接池
int test_singleton_pool()
{
    std::string strErrMsg;
    TestDBPool* pPool = TestDBPool::GetPool();
    if( 2 != pPool->Init("new_elevator/1234@Elevator_8", 2) )
    {
        printf("Initialized database connection pool failed, reason: %s.\n", pPool->GetLastError());
        return -1;
    }
    try
    {
        // 获取当前数据库的时间
        OTL::CDBAppConn conn(pPool);
        if( !conn.Good() )
        {
            printf("Get connection failed, reason: %s.\n", conn.GetLastError());
            return -1;
        }
    
        otl_stream ostr(1, "select to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') from dual", conn);
        char date[32] = { 0 };
        ostr >> date;
        printf("Get current date is : %s.", date);
    }
    catch( otl_exception & e )
    {
        GetErrorInfo(e, strErrMsg);
        printf(strErrMsg.c_str());
    }

    return 0;
}

int test_convert_datetime()
{
    char * str1 = "2012-04-10";
    char * str2 = "2012-04-10 10:10:10";
    char * str3 = "20120410";
    char * str4 = "20120410 10:10:10";

    otl_datetime dt1, dt2, dt3, dt4;
    ConvertOtlDatetime(dt1, str1); printf("[%s], %d-%d-%d, %d:%d:%d\n", str1, dt1.year, dt1.month, dt1.day, dt1.hour, dt1.minute, dt1.second);
    ConvertOtlDatetime(dt2, str2); printf("[%s], %d-%d-%d, %d:%d:%d\n", str2, dt2.year, dt2.month, dt2.day, dt2.hour, dt2.minute, dt2.second);
    ConvertOtlDatetime(dt3, str3); printf("[%s], %d-%d-%d, %d:%d:%d\n", str3, dt3.year, dt3.month, dt3.day, dt3.hour, dt3.minute, dt3.second);
    ConvertOtlDatetime(dt4, str4); printf("[%s], %d-%d-%d, %d:%d:%d\n", str4, dt4.year, dt4.month, dt4.day, dt4.hour, dt4.minute, dt4.second);

    return 0;
}
