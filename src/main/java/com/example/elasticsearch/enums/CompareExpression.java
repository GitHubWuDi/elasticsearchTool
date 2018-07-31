package com.example.elasticsearch.enums;

public enum CompareExpression {
	/// <summary>
    /// 等于
    /// </summary>
    Eq,

    /// <summary>
    ///  where a!=1
    /// </summary>
    NotEq,

    /// <summary>
    /// in (1,2,3)
    /// </summary>
    In,

    /// <summary>
    /// 之间
    /// </summary>
    Between,

    /// <summary>
    /// is not null
    /// </summary>
    NotNull,

    /// <summary>
    /// is null
    /// </summary>
    IsNull,

    /// <summary>
    /// 类似
    /// 不需要加配符号
    /// </summary>
    Like,

    /// <summary>
    /// like '%xx'
    /// 不需要加配符号
    /// </summary>
    LikeEnd,

    /// <summary>
    /// like 'xx%'
    /// 不需要加配符号
    /// </summary>
    LikeBegin,

    /// <summary>
    /// 大于
    /// </summary>
    Gt,

    /// <summary>
    /// 大于等于
    /// </summary>
    Ge,

    /// <summary>
    /// 小于等于
    /// </summary>
    Le,

    /// <summary>
    /// 小于
    /// </summary>
    Lt,

    /// <summary>
    /// 逻辑且
    /// </summary>
    And,

    /// <summary>
    /// 逻辑或者
    /// </summary>
    Or,

    /// <summary>
    /// 逻辑非
    /// </summary>
    Not,
}
