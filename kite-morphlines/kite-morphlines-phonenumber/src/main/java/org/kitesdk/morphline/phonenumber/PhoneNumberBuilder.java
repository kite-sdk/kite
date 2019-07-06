/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.morphline.phonenumber;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import com.typesafe.config.Config;

public class PhoneNumberBuilder implements CommandBuilder {

	private final static String COMMAND_NAME = "phoneNumber";

	@Override
	public Collection<String> getNames() {
		return Collections.singletonList(COMMAND_NAME);
	}

	@Override
	public Command build(final Config config, final Command parent, final Command child,
			final MorphlineContext context) {
		return new PhoneNumber(this, config, parent, child, context);
	}

	private static final class PhoneNumber extends AbstractCommand {

		private final static String FIELD_INPUT_FIELD = "inputField";
		private final static String FIELD_OUTPUT_FIELD_PREFIX = "outputFieldPrefix";
		private final static String FIELD_DEFAULT_REGION_CODE = "defaultRegionCode";
		private final static String FIELD_REGION_CODE_FIELD = "regionCodeField";
		private final static String FIELD_OUTPUT_PHONE_FORMAT = "outputPhoneFormat";

		private final static String DEFAULT_OUTPUT_FIELD_PREFIX = "phone_";
		private final static String DEFAULT_REGION_CODE = "US";
		private final static String DEFAULT_OUTPUT_PHONE_FORMAT = "INTERNATIONAL";

		private final static String SUFFIX_PHONE = "phone";
		private final static String SUFFIX_COUNTRY_CODE = "country_code";
		private final static String SUFFIX_REGION_CODE = "region_code";

		private final String inputField;
		private final String outputPhoneField;
		private final String outputCountryCodeField;
		private final String outputRegionCodeField;
		private final String defaultRegionCode;
		private final String regionCodeField;
		private final PhoneNumberUtil.PhoneNumberFormat phoneNumberFormat;
		private final PhoneNumberUtil phoneNumberUtil;

		public PhoneNumber(final CommandBuilder builder, final Config config, final Command parent, final Command child,
				final MorphlineContext context) {
			super(builder, config, parent, child, context);

			this.phoneNumberUtil = PhoneNumberUtil.getInstance();
			this.inputField = getConfigs().getString(config, FIELD_INPUT_FIELD);

			final String outputFieldPrefix = getConfigs()
					.getString(config, FIELD_OUTPUT_FIELD_PREFIX, DEFAULT_OUTPUT_FIELD_PREFIX);
			this.outputPhoneField = outputFieldPrefix + SUFFIX_PHONE;
			this.outputCountryCodeField = outputFieldPrefix + SUFFIX_COUNTRY_CODE;
			this.outputRegionCodeField = outputFieldPrefix + SUFFIX_REGION_CODE;
			this.regionCodeField = getConfigs().getString(config, FIELD_REGION_CODE_FIELD, null);

			this.defaultRegionCode = getConfigs().getString(config, FIELD_DEFAULT_REGION_CODE, DEFAULT_REGION_CODE);
			if (!this.phoneNumberUtil.getSupportedRegions().contains(this.defaultRegionCode)) {
				throw new MorphlineCompilationException(
						"The specified default region code is not valid: " + this.defaultRegionCode, config);
			}

			final String phoneNumberFormatString = getConfigs()
					.getString(config, FIELD_OUTPUT_PHONE_FORMAT, DEFAULT_OUTPUT_PHONE_FORMAT)
					.toUpperCase(Locale.ENGLISH);
			try {
				this.phoneNumberFormat = PhoneNumberUtil.PhoneNumberFormat.valueOf(phoneNumberFormatString);
			} catch (IllegalArgumentException ex) {
				throw new MorphlineCompilationException(
						"The specified phone number format is not valid: " + phoneNumberFormatString, config);
			}

			validateArguments();
		}

		@Override
		protected boolean doProcess(final Record record) {

			final Object objValue = record.getFirstValue(inputField);
			if (objValue == null) {
				LOG.debug("Input field ({}) was null", inputField);
				return false;
			} else if (!(objValue instanceof String)) {
				if (LOG.isWarnEnabled()) {
					LOG.warn("Input field ({}) has type {} but String was expected", inputField,
							objValue.getClass().getCanonicalName());
				}
				return false;
			}
			final String inputValue = (String) objValue;

			final String regionCode = getRegionCode(record);

			try {
				final Phonenumber.PhoneNumber phoneNumber = phoneNumberUtil.parse(inputValue, regionCode);
				final String formattedPhoneNumber = phoneNumberUtil.format(phoneNumber, phoneNumberFormat);
				final int countryCode = phoneNumber.getCountryCode();
				record.put(outputPhoneField, formattedPhoneNumber);
				record.put(outputCountryCodeField, Integer.toString(countryCode));
				record.put(outputRegionCodeField, phoneNumberUtil.getRegionCodesForCountryCode(countryCode).get(0));
			} catch (NumberParseException ex) {
				LOG.warn("Exception while parsing phone number: " + inputValue, ex);
				return false;
			}

			return super.doProcess(record);
		}

		private String getRegionCode(final Record record) {
			if (regionCodeField == null) {
				return defaultRegionCode;
			}
			final Object objValue = record.getFirstValue(regionCodeField);
			if (objValue == null) {
				LOG.trace("Region code field ({}) was null, defaulting to {}", regionCodeField, defaultRegionCode);
				return defaultRegionCode;
			}
			if (!(objValue instanceof String)) {
				if (LOG.isWarnEnabled()) {
					LOG.warn("Region code field ({}) has type {} but String was expected", regionCodeField,
							objValue.getClass().getCanonicalName());
				}
				return defaultRegionCode;
			}
			final String regionCode = ((String) objValue).toUpperCase(Locale.ENGLISH);
			if (!phoneNumberUtil.getSupportedRegions().contains(regionCode)) {
				LOG.warn("Region code field ({}) got an invalid value: {}", regionCodeField, regionCode);
				return defaultRegionCode;
			}
			return regionCode;
		}

	}

}
