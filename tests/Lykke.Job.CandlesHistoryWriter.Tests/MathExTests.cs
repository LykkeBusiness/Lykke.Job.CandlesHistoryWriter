using System;
using Lykke.Job.CandlesHistoryWriter.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Lykke.Job.CandlesHistoryWriter.Tests;

[TestClass]
public class MathExTests
{
    [TestMethod]
    public void GetPositiveValueOrDefault_WhenValueIsNull_ThenReturnsDefaultValue()
    {
        int? value = null;
        uint defaultValue = 10;

        var result = MathEx.GetPositiveValueOrDefault(value, defaultValue);

        Assert.IsTrue(result == defaultValue);
    }

    [TestMethod]
    public void GetPositiveValueOrDefault_WhenValueIsNegative_ThenReturnsDefaultValue()
    {
        int? value = -1;
        uint defaultValue = 100;

        var result = MathEx.GetPositiveValueOrDefault(value, defaultValue);

        Assert.IsTrue(result == defaultValue);
    }

    [TestMethod]
    public void GetPositiveValueOrDefault_WhenValueIsZero_ThenReturnsDefaultValue()
    {
        int? value = 0;
        uint defaultValue = 1000;

        var result = MathEx.GetPositiveValueOrDefault(value, defaultValue);

        Assert.IsTrue(result == defaultValue);
    }

    [TestMethod]
    public void GetPositiveValueOrDefault_WhenValueIsPositive_ThenReturnsThatValue()
    {
        int? value = new Random().Next(1, 10000);
        uint defaultValue = 10001;

        var result = MathEx.GetPositiveValueOrDefault(value, defaultValue);

        Assert.IsTrue(result == value);
    }

    [TestMethod]
    public void GetPositiveValueOrDefault_WhenDefaultValueIsGreaterThanInt32MaxValue_ThenThrowsOverflowException()
    {
        int? value = null;
        uint defaultValue = uint.MaxValue;

        Assert.ThrowsException<OverflowException>(() => MathEx.GetPositiveValueOrDefault(value, defaultValue));
    }
}