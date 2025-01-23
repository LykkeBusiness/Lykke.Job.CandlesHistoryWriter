// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;

namespace Lykke.Job.CandlesHistoryWriter.Core
{
    public static class MathEx
    {
        /// <summary>
        /// Linear interpolation
        /// </summary>
        public static decimal Lerp(decimal v0, decimal v1, decimal t)
        {
            return (1m - t) * v0 + t * v1;
        }

        /// <summary>
        /// Clamps decimal value by the given boundaries
        /// </summary>
        public static decimal Clamp(decimal value, decimal lowerBound, decimal upperBound)
        {
            if (value < lowerBound)
            {
                return lowerBound;
            }
            if (value > upperBound)
            {
                return upperBound;
            }
            return value;
        }

        /// <summary>
        /// Gets positive value or default
        /// </summary>
        /// <param name="value">Any integer value or null</param>
        /// <param name="defaultValue">Default value to be used in case the original value is negative, 0 or null</param>
        /// <returns>Positive value or 0 (if default value is 0)</returns>
        /// <exception cref="OverflowException">Thrown when the default value is greater than Int32.MaxValue</exception>
        public static int GetPositiveValueOrDefault(this int? value, uint defaultValue) => value switch
        {
            not null when value.Value <= 0 => Convert.ToInt32(defaultValue),
            not null => value.Value,
            _ => Convert.ToInt32(defaultValue)
        };
    }
}
