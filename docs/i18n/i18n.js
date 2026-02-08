/**
 * i18n Helper - Minimal internationalization for Insights Dashboard
 * Supports ES (default) and EN via ?lang=en query parameter
 */

// Detect language: URL param > Navigator language > 'es' (default)
const urlLang = new URLSearchParams(window.location.search).get('lang');
const navLang = navigator.language.split('-')[0]; // 'nl-NL' -> 'nl'
const supportedLangs = ['es', 'en', 'nl', 'da'];
const LANG = urlLang || (supportedLangs.includes(navLang) ? navLang : 'es');

// Translation cache
let translations = {};

/**
 * Load translation file for current language
 */
async function loadTranslations() {
    try {
        const response = await fetch(`i18n/${LANG}.json`);
        if (!response.ok) {
            console.warn(`Translations not found for ${LANG}, falling back to es`);
            const fallbackResponse = await fetch('i18n/es.json');
            translations = await fallbackResponse.json();
        } else {
            translations = await response.json();
        }
    } catch (error) {
        console.error('Failed to load translations:', error);
        translations = {}; // Empty object as ultimate fallback
    }
}

/**
 * Get translated string by key path (e.g., "hero.title")
 * @param {string} key - Dot-notation path to translation
 * @param {object} replacements - Optional object for template replacements
 * @returns {string} Translated string or key if not found
 */
function t(key, replacements = {}) {
    const keys = key.split('.');
    let value = translations;

    // Navigate through nested object
    for (const k of keys) {
        if (value && typeof value === 'object' && k in value) {
            value = value[k];
        } else {
            console.warn(`Translation key not found: ${key}`);
            return key; // Return key itself as fallback
        }
    }

    // Handle template replacements (e.g., "Hello {name}" with {name: "John"})
    if (typeof value === 'string' && Object.keys(replacements).length > 0) {
        return value.replace(/\{(\w+)\}/g, (match, key) => {
            return replacements[key] !== undefined ? replacements[key] : match;
        });
    }

    return value;
}

/**
 * Apply translations to DOM elements with data-i18n attribute
 * Usage: <span data-i18n="hero.title"></span>
 */
function applyTranslations() {
    // Update lang attribute
    document.documentElement.lang = LANG;

    // Apply translations to elements with data-i18n
    document.querySelectorAll('[data-i18n]').forEach(el => {
        const key = el.getAttribute('data-i18n');
        const translation = t(key);

        // Update innerHTML if translation found
        if (translation !== key) {
            el.innerHTML = translation;
        }
    });

    // Apply translations to placeholder attributes
    document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
        const key = el.getAttribute('data-i18n-placeholder');
        const translation = t(key);

        if (translation !== key) {
            el.placeholder = translation;
        }
    });

    // Apply translations to title attributes (tooltips)
    document.querySelectorAll('[data-i18n-title]').forEach(el => {
        const key = el.getAttribute('data-i18n-title');
        const translation = t(key);

        if (translation !== key) {
            el.title = translation;
        }
    });
}

/**
 * Get current locale for date/number formatting
 * @returns {string} Locale code (e.g., 'es-MX', 'en-US', 'nl-NL')
 */
function getLocale() {
    const localeMap = {
        'en': 'en-US',
        'nl': 'nl-NL',
        'da': 'da-DK',
        'es': 'es-MX'
    };
    return localeMap[LANG] || 'es-MX';
}

/**
 * Format date according to current locale
 * @param {Date|string} date - Date to format
 * @param {object} options - Intl.DateTimeFormat options
 * @returns {string} Formatted date string
 */
function formatDate(date, options = {}) {
    const dateObj = typeof date === 'string' ? new Date(date) : date;
    return dateObj.toLocaleDateString(getLocale(), options);
}

/**
 * Format number according to current locale
 * @param {number} num - Number to format
 * @param {object} options - Intl.NumberFormat options
 * @returns {string} Formatted number string
 */
function formatNumber(num, options = {}) {
    return num.toLocaleString(getLocale(), options);
}

/**
 * Initialize i18n system
 * Call this on page load
 */
async function initI18n() {
    await loadTranslations();
    applyTranslations();

    // Emit custom event when translations are ready
    document.dispatchEvent(new CustomEvent('i18n:ready', { detail: { lang: LANG } }));
}

// Auto-initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initI18n);
} else {
    initI18n();
}

// Export functions for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { t, LANG, getLocale, formatDate, formatNumber, initI18n };
}
