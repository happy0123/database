/*****************************************************************************************
File name   : dbpool.h
Author      : Yin Yong
Version     : V1.0
Date        : 2011-11-4
Description : 基于OTL的数据连接池和相关的类
Others      : 
History : 
Date      Author        Version          Modification
---------------------------------------------------------------
Date          Author              Version          Modification
2011-11-4     Yin Yong            V1.0                 created
******************************************************************************************/

#ifndef __YZ_DBPOOL_H__
#define __YZ_DBPOOL_H__

#ifdef _WIN32
    #include <Windows.h>    
#else

#endif

/***********************************************
    Support 64-bit signed integers
************************************************/
#if defined(_MSC_VER) // VC++

    // Enabling support for 64-bit signed integers
    // Since 64-bit integers are not part of the ANSI C++
    // standard, this definition is compiler specific.
    #define OTL_BIGINT __int64

    // Defining a string-to-bigint conversion 
    // that is used by OTL internally.
    // Since 64-bit ineteger conversion functions are
    // not part of the ANSI C++ standard, the code
    // below is compiler specific
    #define OTL_STR_TO_BIGINT(str,n)                \
    {                                               \
        n=_atoi64(str);                               \
    }

    // Defining a bigint-to-string conversion 
    // that is used by OTL internally.
    // Since 64-bit ineteger conversion functions are
    // not part of the ANSI C++ standard, the code
    // below is compiler specific
    #if (_MSC_VER >= 1400) // VC++ 8.0 or higher
    #define OTL_BIGINT_TO_STR(n,str)                \
    {                                               \
        _i64toa_s(n,str,sizeof(str),10);              \
    }
    #else
    #define OTL_BIGINT_TO_STR(n,str)                \
    {                                               \
        _i64toa(n,str,10);                            \
    }
    #endif

#elif defined(__GNUC__) // GNU C++

    #include <stdlib.h>
    #define OTL_BIGINT long long
    #define OTL_STR_TO_BIGINT(str,n)                \
    {                                               \
        n=strtoll(str,0,10);                          \
    }
    #define OTL_BIGINT_TO_STR(n,str)                \
    {                                               \
        sprintf(str,"%lld",n);                        \
    }

#endif // support 64-bit signed integers


#define OTL_ORA11G
#include "otl/otlv4.h"

#include <list>
#include <string>
#include <set>
using namespace std;

/******************************************************************************************/
namespace OTL
{
    // 全局函数
    // 初始化/关闭OTL环境
    void InitEnv(int threadmode = 1); // 默认为多线程模式
    void CloseEnv(void);

    // 拼接OTL异常中的信息
    void GetErrorInfo(const otl_exception& e, string& errInfo);

    // 根据错误码判断是否需要重新连接
    bool CheckErrCodeForReconnect(int errcode);

    // 测试数据库连接
    bool TestConnection(const char* pzConn, std::string* pstrErrMsg = NULL );
    
    // 转换日期时间
    bool ConvertOtlDatetime( otl_datetime& odt, const char* strDT);
    /******************************************************************************************/
    // 线程锁类
    class COTLThreadLock
    {
    private:
#ifdef _WIN32
        CRITICAL_SECTION m_crtlLock;
        //HANDLE m_crtlMutex;
#else
        pthread_mutex_t  m_crtlLock;
#endif

    public:
        COTLThreadLock()
        {
#ifdef _WIN32
            InitializeCriticalSection(&m_crtlLock);
            //m_crtlMutex = CreateMutex(NULL, FALSE, NULL);
#else
            pthread_mutex_init( &m_crtlLock, NULL );
#endif
        }
        virtual ~COTLThreadLock()
        {
#ifdef _WIN32
            DeleteCriticalSection(&m_crtlLock);
            //CloseHandle( m_crtlMutex );
#else
            pthread_mutex_destroy( &m_crtlLock );
#endif
        }

        void Lock()
        {
#ifdef _WIN32
            EnterCriticalSection(&m_crtlLock);
            //int ret = WaitForSingleObject( m_crtlMutex, INFINITE);
#else
            pthread_mutex_lock( &m_crtlLock );
#endif
        }
        void Unlock()
        {
#ifdef _WIN32
            LeaveCriticalSection(&m_crtlLock);
            //int ret = ReleaseMutex( m_crtlMutex );
#else
            pthread_mutex_unlock( &m_crtlLock );
#endif
        }
    };
    /******************************************************************************************/
    // 数据库连接类
    class CDBConn
    {
    public:
        CDBConn();
        virtual ~CDBConn();

        // 连接/重连/关闭连接
        bool Connect(const char *conn_str);
        bool Reconnect(bool bForce = false);
        void Close(void);

        // 通过异常获取错误信息字符串
        const char* GetErrFromException(const otl_exception& e);

        // 是否需要重新连接
        bool IsNeedReconnect(void);

        // 是否连接上
        inline bool IsConnected(void) { return (m_db.connected == 1 ? true : false); }

        // 获取错误信息
        inline const char* GetLastError(void) { return m_strErrMsg.c_str(); }

        // 获取OTL连接对象
        inline otl_connect& GetDb(void) {return m_db;};

        // 设置异常
        inline void SetException(const otl_exception& e) { m_err = e; }

    private:
        otl_connect  m_db;
        otl_exception m_err;
        std::string  m_strConn;
        std::string  m_strErrMsg;
    };
    /******************************************************************************************/
    // 连接池类
    class CDBConnPool
    {
    public:
        CDBConnPool();
        virtual ~CDBConnPool();

        // 初始化/销毁连接池
        int Init(const char *conn_str, int conn_num, int auto_add_num = 2);
        void Destroy(void);

        // 获取/释放连接
        CDBConn *GetConn(bool bAutoAdd = true);
        void ReleaseConn(CDBConn *conn);

        // 连接数控制操作
        int AddConnNum(int num);
        void ReduceConnNum(int num);
        inline void SetAutoConnNum(unsigned int num) { m_nAutoAddConnNum = num; }
        inline int GetConnNum(void) { return (int)m_ConnList.size(); }

        // 设置连接池中的连接的异常信息
        void SetAllConnExceptions(const otl_exception& e );

        // 获取错误信息
        inline const char* GetLastError(void) { return m_strErrMsg.c_str(); }

    private:
        std::list<CDBConn*>  m_ConnList;        // connection objects's list
        std::string          m_strConn;         // connection characters
        std::string          m_strErrMsg;       // connected error message
        unsigned int         m_nAutoAddConnNum; // adding number automatically
        COTLThreadLock       m_Lock;            // thread lock
    };
    
    /******************************************************************************************/
    // 数据库连接应用类
    class CDBAppConn
    {
    public:
        CDBAppConn(CDBConnPool *pPool);
        ~CDBAppConn();

        void Release(void); // release the db connection
        void Commit(void);  // commit a transaction manually
        bool Rollback(otl_exception* pException = NULL); // rollback a transaction manually

        // 连接是否可用
        inline bool Good(void){ return (m_pConn && m_pConn->IsConnected()); }

        // 重新连接，当不可用时进行尝试
        inline bool Reconnect(bool bForce = false) { return (m_pConn ? m_pConn->Reconnect(bForce) : false); }

        // 获取错误信息
        inline const char* GetLastError(void) { return (m_pConn ? m_pConn->GetLastError() : "NULL Connection"); }

        // 从异常中获取错误信息
        const char* GetErrFromException(const otl_exception& e);

        // 获取OTL连接对象
        operator otl_connect&(void) const;

    private:
        CDBConn     * m_pConn;
        CDBConnPool * m_pPool;
    };
    /******************************************************************************************/
    // 单件连接池类
    class CDBSingletonConnPool : public CDBConnPool
    {
    public:
        virtual ~CDBSingletonConnPool() {};
        static inline CDBSingletonConnPool* GetPool(void) { return &m_SingletonPool; }
    private:
        CDBSingletonConnPool(){};
        static CDBSingletonConnPool m_SingletonPool;
    };

    // 声明和定义单件模式的专用数据库连接池
#define SINGLETON_DBCONNPOOL_DECLARE(ClassName) \
    class ClassName : public OTL::CDBConnPool \
    { \
    public: \
        virtual ~ ClassName() {}; \
        static inline ClassName * GetPool(void) { return &m_SingletonPool; } \
    private: \
        ClassName(){}; \
        static ClassName m_SingletonPool; \
    }

// SINGLETON_DBCONNPOOL_DECLARE
#define SINGLETON_DBCONNPOOL_DEFINE(ClassName) \
    ClassName ClassName ::m_SingletonPool
// SINGLETON_DBCONNPOOL_DEFINE

    /******************************************************************************************/

} // namespace OTL
/******************************************************************************************/

#endif
