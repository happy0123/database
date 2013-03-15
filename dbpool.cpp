/*****************************************************************************************
File name   : dbpool.cpp
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

#include "dbpool.h"

#ifdef OS_WINDOWS

#ifndef _UTF8_
#define _UTF8_
#endif
#include "../common/CharConvert.h"

#endif

/******************************************************************************************/

namespace OTL
{
    /*****************************************************************
    Function    : InitEnv
    Description : 初始化OTL环境
    Input       : 
        @ threadmode : 1为多线程模式, 0则不是
    Output      : 
    Return      : 
    ******************************************************************/
    void InitEnv(int threadmode /* = 1 */)
    {
        static bool bNotInit = true;
        if( bNotInit )
        {
            otl_connect::otl_initialize(threadmode);
            bNotInit = false;
        }
    }
    /*****************************************************************
    Function    : CloseEnv
    Description : 关闭OTL环境
    Input       : 
    Output      : 
    Return      : 
    ******************************************************************/
    void CloseEnv(void)
    {
        otl_connect::otl_terminate();
    }
    /*****************************************************************
    Function    : GetErrorInfo
    Description : 拼接OTL的异常信息字符串
    Input       : 
        @ e     : OTL异常对象
        @ errInfo : 存储异常信息
    Output      : errInfo
    Return      : 
    ******************************************************************/
    void GetErrorInfo(const otl_exception& e, string& errInfo)
    {
#ifdef _UTF8_
        errInfo = "OTL_EXCEPTION: ";
        errInfo += CWCharToChar((char*)e.msg, E_UTF8, E_CHAR).Char();
#ifdef _DEBUG
        if( e.stm_text[0] != 0 )
            errInfo += string(",\n SQL: ") + CWCharToChar(e.stm_text, E_UTF8, E_CHAR).Char();
#endif
        if( e.var_info[0] != 0 )
            errInfo += string(",\n VARINFO: ") + CWCharToChar(e.var_info, E_UTF8, E_CHAR).Char();

#else  // undef _UTF8_  /////////////////////////////////

        errInfo = "OTL_EXCEPTION: ";
        errInfo += (char*)e.msg;
#ifdef _DEBUG
        if( e.stm_text[0] != 0 )
            errInfo += string(",\n SQL: ") + e.stm_text;
#endif
        if( e.var_info[0] != 0 )
            errInfo += string(",\n VARINFO: ") + e.var_info;
#endif   
    }
    /*****************************************************************
    Function    : IsNeedReconnect
    Description : 根据错误码判断是否需要重新连接
    Input       : 
        @ errcode : OTL异常中的错误码
    Output      : 
    Return      : 
        需要连接：true
        不需要  : false
    ******************************************************************/
    bool CheckErrCodeForReconnect(int errcode)
    {
        static std::set<int> setCode;

        // 初始化错误码集合
        if( setCode.empty() )
        {
            setCode.insert(3113);   // ORA-03113: 通信通道的文件结尾
            setCode.insert(3114);   // ORA-03114: 未连接到 ORACLE
            setCode.insert(3135);   // ORA-03135: 失去联系
            setCode.insert(12541);  // ORA-12541: TNS 无监听程序（需要启动监听后再重新连接）
        }

        // 判断错误码
        if( setCode.find( errcode ) != setCode.end() )
            return true;
        else
            return false;
    }
    /*****************************************************************
    Function    : TestConnection
    Description : 测试数据库连接
    Input       : 
        @ pzConn     : 连接字符串
        @ pstrErrMsg : 存储异常信息
    Output      : pstrErrMsg
    Return      : 
        成功    ： true
        失败    ： false 
    ******************************************************************/
    bool TestConnection(const char* pzConn, std::string* pstrErrMsg /* = NULL */)
    {
        InitEnv();
        otl_connect db;

        try
        {   
            db.rlogon(pzConn);
        }
        catch( otl_exception & e )
        {
            if( pstrErrMsg )
                GetErrorInfo(e, *pstrErrMsg);
        }

        return ( db.connected == 1 ) ? true : false;
    }
    /*****************************************************************
    Function    : ConvertOtlDatetime
    Description : 转换日期时间
    Input       : 
        @ odt   : otl的日期时间类型
        @ strDT : 日期时间字符串
    Output      : odt
    Return      : 
        成功    ： true
        失败    ： false 
    ******************************************************************/
    bool ConvertOtlDatetime( otl_datetime& odt, const char* strDT)
    {
        if( !strDT || !*strDT ) return false;

        // 字符串的日期时间格式：YYYY-MM-DD HH:mi:ss  OR  YYYY-MM-DD
        size_t len = strlen(strDT);
        if( 10 != len && 19 != len ) return false;

        char buf[8] = {0};
        memcpy(buf, strDT, 4 );   buf[4] = 0; odt.year  = atoi(buf);
        memcpy(buf, strDT+5, 2 ); buf[2] = 0; odt.month = atoi(buf);
        memcpy(buf, strDT+8, 2 );             odt.day   = atoi(buf);

        if( 19 == strlen(strDT))
        {
            memcpy(buf, strDT+11, 2 );       odt.hour   = atoi(buf);
            memcpy(buf, strDT+14, 2 );       odt.minute = atoi(buf);
            memcpy(buf, strDT+17, 4 );       odt.second = atoi(buf);
        }
        return true;
    }
    /*****************************************************************

        CDBConn 连接类

    *****************************************************************/
    /*****************************************************************
    Function    : CDBConn::CDBConn
    Description : 构造函数，初始化OCL环境
    Input       : 
    Output      : 无
    Return      : 
    ******************************************************************/
    CDBConn::CDBConn()
    {
        InitEnv();
    }
    /*****************************************************************
    Function    : CDBConn::~CDBConn
    Description : 析构函数，关闭连接
    Input       : 
    Output      : 无
    Return      : 
    ******************************************************************/
    CDBConn::~CDBConn()
    {
        Close();
    }
    /*****************************************************************
    Function    : CDBConn::Connect
    Description : 连接数据库,且显示设置不自动提交
    Input       : 
        @ conn_str  ： 连接字符串(格式: username/password@{ORACLE Alias Name}
    Output      : 无
    Return      : 
        成功    ： true
        失败    ： false 
    ******************************************************************/
    bool CDBConn::Connect( const char *conn_str )
    {
        m_strConn = conn_str;
  	try
		{
            m_db.rlogon(conn_str, 0); //连接数据库，且不自动提交
        }
        catch( otl_exception & e )
        {
            GetErrFromException(e);
        }

        return ( m_db.connected == 1 ) ? true : false;
    }
    /*****************************************************************
    Function    : CDBConn::Reconnect
    Description : 重新连接数据库
    Input       : 
        @ bForce ： 强制重练标识，如果为false则判断是否已经连接上，是则返回
    Output      : 无
    Return      : 
        成功    ： true
        失败    ： false
    ******************************************************************/
    bool CDBConn::Reconnect(bool bForce /* = false */)
    {
        if( m_db.connected == 1 && !bForce )
            return true;

        try
        {
            //m_db.session_end();
            //m_db.session_reopen();  // need test it
            m_db.logoff();
            m_db.rlogon(m_strConn.c_str());
        }
        catch( otl_exception & e )
        {
            GetErrFromException(e);
        }

        return ( m_db.connected == 1 ) ? true : false;
    }
    /*****************************************************************
    Function    : CDBConn::Close
    Description : 关闭连接
    Input       :     
    Output      : 无
    Return      : 
    ******************************************************************/
    void CDBConn::Close()
    {
        try
        {
            if( m_db.connected == 1 )
                m_db.logoff();
        }
        catch( otl_exception & e )
        {
            GetErrFromException(e);
        }

        m_db.connected = 0;
    }
    /*****************************************************************
    Function    : CDBConn::IsNeedReconnect
    Description : 判断是否需要重新连接
    Input       :     
    Output      : 无
    Return      : 
        需要    ： true
        不需要  ： false
    ******************************************************************/
    bool CDBConn::IsNeedReconnect(void)
    {
        if( ( m_db.connected == 0 ) // 未连接
            || ( m_db.connected == 1 && CheckErrCodeForReconnect(m_err.code) ) ) // 连接异常，需要重新连接
            return true;
        else
            return false;
    }
    /*****************************************************************
    Function    : CDBConn::GetErrFromException
    Description : 通过异常获取错误信息字符串
    Input       : 
        @ e     : 调用时产生的错误异常对象
    Output      : 无
    Return      : 错误信息字符串
    ******************************************************************/
    const char* CDBConn::GetErrFromException(const otl_exception& e)
    {
        m_err = e;
        GetErrorInfo(e, m_strErrMsg);
        return m_strErrMsg.c_str();
    }
    /*****************************************************************

        CDBSingletonConnPool 单件连接池类

    *****************************************************************/
    CDBSingletonConnPool CDBSingletonConnPool::m_SingletonPool;

    /*****************************************************************

        CDBConnPool 连接池类

    *****************************************************************/
    //CDBConnPool CDBConnPool::m_Pool;

    /*****************************************************************
    Function    : CDBConnPool::CDBConnPool
    Description : 构造函数，初始化OTL环境
    ******************************************************************/
    CDBConnPool::CDBConnPool()
    {
        InitEnv();
    }
    /*****************************************************************
    Function    : CDBConnPool::~CDBConnPool
    Description : 析构函数，销毁连接池，关闭OTL
    ******************************************************************/
    CDBConnPool::~CDBConnPool()
    {
        Destroy();
    }
    /*****************************************************************
    Function    : CDBConnPool::Init
    Description : 初始化连接池
    Input       : 
        @ conn_str     ： 连接字符串
        @ conn_num     ： 连接数量
        @ auto_add_num :  自动增加连接数量
    Output      : 无
    Return      : 
        成功    ： >0 (=conn_num完全成功)
        失败    ： 0    
    ******************************************************************/
    int CDBConnPool::Init( const char *conn_str, int conn_num, int auto_add_num /* = 2 */ )
    {
        if ( m_ConnList.size() > 0 ) // 防止重复初始化
            return m_ConnList.size();

        m_strConn = conn_str;
        m_nAutoAddConnNum = auto_add_num;

        //otl_connect::otl_initialize(1); // initialize OCI environmen

        for (int i = 0; i < conn_num; ++i)
        {
            CDBConn * pConn = new CDBConn;
            if( !pConn->Connect(conn_str) )
            {
                m_strErrMsg = pConn->GetLastError();
                delete pConn;
                break;
            }
            m_ConnList.push_back(pConn);
        }

        return m_ConnList.size();
    }
    /*****************************************************************
    Function    : CDBConnPool::Destroy
    Description : 销毁连接池
    Input       : 
    Output      : 无
    Return      : 
    ******************************************************************/
    void CDBConnPool::Destroy(void)
    {
        list<CDBConn*>::iterator it ;
        for (it = m_ConnList.begin(); it != m_ConnList.end(); ++it)
        {
            delete (*it);
        }
        m_ConnList.clear();
    }
    /*****************************************************************
    Function    : CDBConnPool::GetConn
    Description : 从连接池中获取一个连接
    Input       : 
        @ bAutoAdd ： 是否自动增加连接
    Output      : 无
    Return      : 
        成功    ： 连接指针
        失败    ： NULL
    ******************************************************************/
    CDBConn * CDBConnPool::GetConn(bool bAutoAdd /* = true */)
    {
        CDBConn *pConn = NULL;
        m_Lock.Lock();

        if (m_ConnList.size() == 0)
        {
            if( !bAutoAdd || AddConnNum(m_nAutoAddConnNum) <= 0 )
            {
                m_Lock.Unlock();
                return NULL;
            }
        }
        pConn = m_ConnList.front();
        m_ConnList.pop_front();

        m_Lock.Unlock();
        return pConn;
    }
    /*****************************************************************
    Function    : CDBConnPool::ReleaseConn
    Description : 释放连接，返回到连接池中
    Input       : 
        @ pConn ： 连接对象指针
    Output      : 无
    Return      : 
    ******************************************************************/
    void CDBConnPool::ReleaseConn( CDBConn *pConn )
    {
        m_Lock.Lock();
        m_ConnList.push_back(pConn);
        m_Lock.Unlock();
    }
    /*****************************************************************
    Function    : CDBConnPool::AddConnNum
    Description : 增加连接池中的连接
    Input       : 
        @ num   ： 连接数量    
    Output      : 无
    Return      :   
        成功    ： > 0 (=num完全成功)
        失败    ： <= 0    
    ******************************************************************/
    int CDBConnPool::AddConnNum(int num)
    {
        if( m_strConn.empty() )
        {
            m_strErrMsg = "Not initialization!";
            return -1;
        }
        int i = 0;
        for ( i = 0; i < num; ++i)
        {
            CDBConn *pConn = new CDBConn;
            if( !pConn->Connect(m_strConn.c_str()) )
            {
                m_strErrMsg = pConn->GetLastError();
                delete pConn;
                break;
            }
            m_ConnList.push_back(pConn);
        }
        return i;
    }
    /*****************************************************************
    Function    : CDBConnPool::ReduceConnNum
    Description : 减少连接池中的连接
    Input       : 
        @ num   ： 连接数量    
    Output      : 无
    Return      :     
    ******************************************************************/
    void CDBConnPool::ReduceConnNum(int num)
    {
        int size = (int)m_ConnList.size();
        size = (num <= size) ? num : size;
        for( int i = 0; i < size; ++i)
        {
            CDBConn *pConn = GetConn(false);

            if( NULL != pConn )
                delete pConn;
            else
                break;
        }
    }

    /*****************************************************************
    Function    : CDBConnPool::SetAllConnExceptions
    Description : 设置连接池中的连接的异常信息(当网络断开或者数据库服务器出现异常时)
    Input       : 
        @ e     ： 外部异常对象
    Output      : 无
    Return      :
    ******************************************************************/
    void CDBConnPool::SetAllConnExceptions(const otl_exception& e )
    {
        m_Lock.Lock();
        list<CDBConn*>::iterator iter = m_ConnList.begin();
        for( ; iter != m_ConnList.end(); iter++ )
            (*iter)->SetException(e);
        m_Lock.Unlock();
    }

    /*****************************************************************
        
        CDBAppConn 应用连接类(已连接好)

    ******************************************************************/
    /*****************************************************************
    Function    : CDBAppConn::CDBAppConn
    Description : 构造函数
    Input       : 
        @ pPool : 连接池指针
    Output      : 
    Return      :
    ******************************************************************/
    CDBAppConn::CDBAppConn(CDBConnPool *pPool)
        : m_pConn(NULL)
        , m_pPool(NULL)
    {
        m_pPool = pPool;
        m_pConn = pPool->GetConn();

        if( m_pConn && m_pConn->IsNeedReconnect() )  // 没有连接上或连接异常则重新连接
            m_pConn->Reconnect(true);
    }
    /*****************************************************************
    Function    : CDBAppConn::~CDBAppConn
    Description : 析构函数
    ******************************************************************/
    CDBAppConn::~CDBAppConn()
    {
        Release();
    }
    /*****************************************************************
    Function    : CDBAppConn::otl_connect
    Description : 获取otl_connect对象
    Input       : 
    Output      : 
    Return      :     
    ******************************************************************/
    CDBAppConn::operator otl_connect&(void) const
    {
        if (!m_pConn)
        {
            throw runtime_error("cannot get database connection ");
        }
        return m_pConn->GetDb();
    }
    /*****************************************************************
    Function    : CDBAppConn::Release
    Description : 释放连接
    Input       : 
    Output      : 
    Return      :     
    ******************************************************************/
    void CDBAppConn::Release(void)
    {
        if (m_pConn)
        {
            m_pPool->ReleaseConn(m_pConn);
            m_pConn = NULL;
        }
    }
    /*****************************************************************
    Function    : CDBAppConn::Commit
    Description : 提交事务（在调用时需要捕捉异常）
    Input       : 
    Output      : 
    Return      :     
    ******************************************************************/
    void CDBAppConn::Commit(void)
    {
        if( !m_pConn )
        {
            throw runtime_error("cannot get database connection ");
        }
        m_pConn->GetDb().commit();
    }
    /*****************************************************************
    Function    : CDBAppConn::Rollback
    Description : 回滚事务（调用时不需要捕捉异常）
    Input       : pException 异常
    Output      : 
    Return      : true: success, false: failed
    ******************************************************************/
    bool CDBAppConn::Rollback(otl_exception* pException /* = NULL */)
    {
        // 根据错误码判断是否需要Rollback
        if( pException && CheckErrCodeForReconnect(pException->code) )
            return true;

        if( !m_pConn )
        {
            throw runtime_error("cannot get database connection ");
        }

        try
        {
            m_pConn->GetDb().rollback();
        }
        catch (otl_exception& e)
        {
            if( pException ) *pException = e;
            return false;
        }
        return true;
    }

    /*****************************************************************
    Function    : CDBAppConn::GetErrFromException
    Description : 通过异常获取错误信息字符串
    Input       : 
        @ e     : 调用时产生的错误异常对象
    Output      : 无
    Return      : 错误信息字符串
    ******************************************************************/
    const char* CDBAppConn::GetErrFromException(const otl_exception& e)
    {
        // 是否都需要设置异常，当网络断开或者数据库服务器出现异常时
        if( m_pPool && m_pPool->GetConnNum() > 0 && CheckErrCodeForReconnect(e.code) )
        {
            m_pPool->SetAllConnExceptions(e);
        }
        return m_pConn ? m_pConn->GetErrFromException(e) : "NULL Connection";
    }
    /******************************************************************************************/
}

/******************************************************************************************/

