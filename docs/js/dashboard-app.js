        // i18n fallback function (in case i18n.js fails to load)
        window.t = window.t || function(key, replacements) {
            console.warn('i18n not loaded, using fallback for key:', key);
            // Return the Spanish text as fallback (original language)
            const fallbackTexts = {
                'dashboard.header_title': 'Insights Dashboard',
                'dashboard.kpis.ads_with_spend': 'Anuncios con Inversión',
                'dashboard.kpis.total_spend': 'Inversión Total',
                'dashboard.accounts.all': 'Todas las cuentas'
            };
            return fallbackTexts[key] || key;
        };

        // State filter compte - DÉCLARÉ EN PREMIER
        let currentAccountName = 'Todos';
        
        // Le parser est maintenant dans nomenclature_parser.js
        // Compatibilité : remplacer l'ancien parseAdName
        const parseAdName = window.NOMEN_V2.parseAdName;

        function buildIndexes(ads, getOverrides) {
            // NEW: alimente le lexique dynamique des créateurs
            if (Array.isArray(ads)) window.NOMEN_V2.setKnownCreatorsFromAds(ads);
            
            const angle = new Map(); 
            const creator = new Map();
            let parsedOK = 0, active = 0;

            for (const ad of ads) {
                const spend = +ad.spend || 0;
                if (spend <= 0) continue;  // cohérent avec vos KPI
                active++;

                const overrides = getOverrides?.(ad.ad_id);
                const p = parseAdName(ad.ad_name, overrides);
                if (p.angle || p.creator) parsedOK++;

                const add = (key, map) => {
                    if (!key) return;
                    const m = map.get(key) || { ads: 0, spend: 0, purchases: 0, revenue: 0 };
                    m.ads++; 
                    m.spend += spend; 
                    m.purchases += (ad.purchases || 0); 
                    m.revenue += (ad.purchase_value || 0);
                    map.set(key, m);
                };

                add(p.angle || 'OTROS', angle);
                add(p.creator || 'OTROS', creator);
            }

            // dérivées pondérées
            const finish = (map) => {
                for (const m of map.values()) {
                    m.roas = m.spend > 0 ? m.revenue / m.spend : 0;
                    m.cpa = m.purchases > 0 ? m.spend / m.purchases : 0;
                }
            };
            finish(angle); 
            finish(creator);

            const coverage = active > 0 ? (parsedOK / active) * 100 : 0;
            const score = (coverage >= 70 ? 40 : coverage >= 40 ? 20 : 0) + 
                          (active > 0 ? ([...angle.keys()].filter(k => k !== 'OTROS').length > 0 ? 30 : 10) : 0) +
                          (coverage >= 50 && [...creator.keys()].some(k => k !== 'OTROS') ? 30 : 10);

            return { angle, creator, score: Math.min(score, 100), coverage };
        }
        // ======================================================================
        // FIN NOMENCLATURE SERVICE
        // ======================================================================
        
        // Helper: try multiple paths for local JSON
        async function loadJSON(paths) {
            // Simplifié: source de vérité = data/current
            const path = Array.isArray(paths) ? paths[0] : paths;
            const res = await fetch(path);
            if (!res.ok) throw new Error('Load failed: ' + path);
            return await res.json();
        }
        
        // Helper functions for media URL handling
        function getMediaUrl(ad) {
            return ad.media_url || ad.media || ad.video_url || ad.image_url || '';
        }
        
        function escapeAttr(s) {
            return String(s ?? '').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;');
        }
        // Charger toutes les données au démarrage
        // NOTE: periodsData is populated by data_loader_saas.js into window.periodsData
        // All code now uses window.periodsData directly (no local variable)
        window.periodsData = window.periodsData || {};  // Initialize if not already done
        let prevWeekData = null;  // Initialize as null, not empty object
        let currentPeriod = 7;
        
        // Fonction d'échappement HTML pour sécurité XSS
        const escapeHtml = (text) => {
            const div = document.createElement('div');
            div.textContent = text || '';
            return div.innerHTML;
        };
        
        // Fonction CSV sécurisée contre injection
        const escapeCsv = (text) => {
            let s = String(text ?? '');
            // Protection contre CSV injection Excel
            if (/^[=+\-@]/.test(s)) s = "'" + s;
            if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
            return s;
        };
        // currentAccount supprimé - on utilise currentAccountName partout
        
        // Constantes centralisées pour les seuils ROAS
        const ROAS_THRESHOLDS = {
            HIGH: 2.0,    // Vert
            MEDIUM: 1.2,  // Jaune
            LOW: 0        // Rouge
        };
        const ROAS_COLORS = {
            HIGH: '#4CAF50',
            MEDIUM: '#FF9800',
            LOW: '#f44336'
        };
        
        // === Tipo de Hook (manual, hors nomenclature) ===
        const HOOK_OPTIONS = [
            'Etiquetas',
            'Preguntas',
            'Condicionales',
            'Comandos',
            'Declaraciones',
            'Listas o Pasos',
            'Narrativas',
            'Exclamaciones / Provocaciones'
        ];
        
        // Removed unused petcareData variable
        let accountsIndex = null; // Index complet des comptes si disponible

        /**
         * Format monetary value with the correct currency based on selected account
         * @param {number} amount - The monetary value
         * @param {object} options - { decimals: number, accountName: string }
         * @returns {string} Formatted string like "$1,234" or "€1,234.50"
         */
        function formatMoney(amount, options = {}) {
            const decimals = options.decimals ?? 0;
            const accountName = options.accountName || currentAccountName;
            const currencyMap = window.accountsCurrency || {};

            // Get currency for this account (default to $ if unknown or multi-account view)
            let currency = currencyMap[accountName] || null;

            // Currency symbol mapping
            const symbols = {
                'USD': '$',
                'MXN': '$',
                'EUR': '€',
                'GBP': '£',
                'ARS': '$',
                'COP': '$',
                'BRL': 'R$',
                'PEN': 'S/',
                'CLP': '$'
            };

            const symbol = (currency && symbols[currency]) ? symbols[currency] : '$';

            // Format amount
            if (decimals > 0) {
                return `${symbol}${amount.toFixed(decimals)}`;
            }
            return `${symbol}${Math.round(amount).toLocaleString()}`;
        }

        /**
         * Get the currency code for the current account
         * @returns {string} Currency code (USD, MXN, EUR) or empty string
         */
        function getCurrentCurrency() {
            const currencyMap = window.accountsCurrency || {};
            if (currentAccountName === 'Todos' || currentAccountName === 'Todas las cuentas' || !currentAccountName) {
                return ''; // Mixed currencies
            }
            return currencyMap[currentAccountName] || '';
        }

        // ⏳ Loading Overlay Controller
        const loadingOverlay = {
            messages: [
                { textKey: 'dashboard.loading_messages.connecting', subKey: 'dashboard.loading_messages.connecting_sub', progress: 10 },
                { textKey: 'dashboard.loading_messages.loading_accounts', subKey: 'dashboard.loading_messages.loading_accounts_sub', progress: 30 },
                { textKey: 'dashboard.loading_messages.processing_metrics', subKey: 'dashboard.loading_messages.processing_metrics_sub', progress: 60 },
                { textKey: 'dashboard.loading_messages.preparing_view', subKey: 'dashboard.loading_messages.preparing_view_sub', progress: 85 },
            ],
            currentIndex: 0,
            interval: null,
            startTime: null,

            start() {
                this.startTime = Date.now();
                this.currentIndex = 0;
                this.update();
                // Cambiar message toutes les 3 secondes
                this.interval = setInterval(() => {
                    if (this.currentIndex < this.messages.length - 1) {
                        this.currentIndex++;
                        this.update();
                    }
                }, 3000);
            },

            update() {
                const msg = this.messages[this.currentIndex];
                const textEl = document.getElementById('loading-text');
                const subEl = document.getElementById('loading-subtext');
                const progressEl = document.getElementById('loading-progress-bar');
                if (textEl) textEl.textContent = t(msg.textKey);
                if (subEl) subEl.textContent = t(msg.subKey);
                if (progressEl) progressEl.style.width = msg.progress + '%';
            },

            hide() {
                if (this.interval) clearInterval(this.interval);
                const elapsed = ((Date.now() - this.startTime) / 1000).toFixed(1);
                console.log(`⏱️ Tiempo de carga total: ${elapsed}s`);

                // Animation finale
                const progressEl = document.getElementById('loading-progress-bar');
                if (progressEl) progressEl.style.width = '100%';

                setTimeout(() => {
                    const overlay = document.getElementById('loading-overlay');
                    if (overlay) overlay.classList.add('hidden');
                }, 300);
            }
        };

        async function loadAllData() {
            // ⏳ Démarrer l'overlay de chargement
            loadingOverlay.start();

            try {
                // Load optimized data
                console.log('📦 Loading optimized data...');
                const result = await loadOptimizedData();

                // Handle new return format: { success, noData, error }
                if (!result.success) {
                    if (result.noData) {
                        // First-time user: no data yet, show waiting UI + auto-refresh
                        console.log('🆕 First-time user detected: no data available yet');
                        loadingOverlay.hide(); // Cacher l'overlay avant d'afficher l'UI nouvel utilisateur
                        showDataPendingUI();
                        return;
                    } else {
                        // Other error (network, auth, etc.)
                        throw new Error(result.error || 'Failed to load optimized data');
                    }
                }

                // Copy from window.periodsData to local periodsData
                periodsData = window.periodsData;
                console.log('✅ Optimized data loaded:', Object.keys(periodsData));

                // Load account currencies for proper monetary display
                if (typeof loadAccountsCurrency === 'function') {
                    await loadAccountsCurrency();
                }

                // Previous week data is generated by the adapter - LOAD BEFORE updateDashboard
                if (window.prevWeekData) {
                    prevWeekData = window.prevWeekData;
                    console.log('✅ Previous week data loaded');
                } else {
                    console.log('Previous week data not available');
                }

                // Display immediately with 7-day data - AFTER loading prevWeekData
                if (window.periodsData[7]) {
                    updateDashboard(7);
                    console.log('✅ 7j loaded and displayed');
                }

                // Update the date with data
                updateDataDate();

                // Check Petcare data availability (optional)
                // Skip for optimized version

                // Accounts index - skip for optimized version
                accountsIndex = null;

                console.log('📊 Datos cargados:', Object.keys(periodsData), 'prev_week');

                // ⏳ Cacher l'overlay - données chargées !
                loadingOverlay.hide();

                // 🎓 Initialize onboarding (welcome modal for first-time users)
                initOnboarding();

            } catch (error) {
                console.error('❌ Error al cargar datos:', error);
                loadingOverlay.hide(); // Cacher même en cas d'erreur
                showErrorUI(error.message);
            }
        }

        // 🆕 Premium UI for first-time users with Video (Wait Marketing)
        async function showDataPendingUI() {
            console.log('🎬 First-time user: showing Wait Marketing video UI');

            // Show the overlay
            const overlay = document.getElementById('firstload-overlay');
            if (overlay) overlay.classList.add('visible');

            // Load video only when overlay is shown (prevent autoplay on every page load)
            const videoIframe = document.getElementById('firstload-video');
            if (videoIframe && videoIframe.dataset.src && !videoIframe.src) {
                videoIframe.src = videoIframe.dataset.src;
                console.log('🎥 Video loaded for first-time user');
            }

            // UI elements
            const titleEl = document.getElementById('firstload-title');
            const subtitleEl = document.getElementById('firstload-subtitle');
            const textEl = document.getElementById('firstload-text');
            const progressBar = document.getElementById('firstload-progress-bar');
            const skipBtn = document.getElementById('firstload-skip-btn');
            const readyBtn = document.getElementById('firstload-ready-btn');

            // Show skip button after 5 seconds
            setTimeout(() => {
                if (skipBtn) skipBtn.style.display = 'inline-block';
            }, 5000);

            // Skip button just continues watching but shows we're still loading
            if (skipBtn) {
                skipBtn.onclick = () => {
                    skipBtn.style.display = 'none';
                    if (subtitleEl) subtitleEl.textContent = t('dashboard.first_load.keep_watching');
                };
            }

            // Progress messages (simpler, shown below video)
            const messages = [
                { textKey: 'dashboard.loading_messages.connecting_meta', progress: 5 },
                { textKey: 'dashboard.loading_messages.downloading_campaigns', progress: 15 },
                { textKey: 'dashboard.loading_messages.analyzing_ads', progress: 25 },
                { textKey: 'dashboard.loading_messages.calculating_metrics', progress: 35 },
                { textKey: 'dashboard.loading_messages.processing_historical', progress: 45 },
                { textKey: 'dashboard.loading_messages.organizing_info', progress: 55 },
                { textKey: 'dashboard.loading_messages.optimizing_data', progress: 65 },
                { textKey: 'dashboard.loading_messages.finalizing', progress: 75 },
                { textKey: 'dashboard.loading_messages.almost_ready', progress: 85 },
                { textKey: 'dashboard.loading_messages.final_details', progress: 90 },
            ];

            let messageIndex = 0;
            const startTime = Date.now();

            // Update messages every 8 seconds
            const messageInterval = setInterval(() => {
                messageIndex = Math.min(messageIndex + 1, messages.length - 1);
                const msg = messages[messageIndex];
                if (textEl) textEl.textContent = t(msg.textKey);
                if (progressBar) progressBar.style.width = msg.progress + '%';
            }, 8000);

            // Trigger refresh for all accounts
            console.log('🔄 Triggering BASELINE refresh for all accounts...');

            try {
                const refreshResult = await triggerRefreshAll();
                if (refreshResult.success) {
                    console.log('✅ BASELINE refresh triggered:', refreshResult);
                } else {
                    console.error('❌ Failed to trigger refresh:', refreshResult.error);
                    clearInterval(messageInterval);
                    if (textEl) textEl.textContent = refreshResult.error || t('dashboard.errors.connection_error');
                    return;
                }
            } catch (error) {
                console.error('❌ Refresh error:', error);
                clearInterval(messageInterval);
                if (textEl) textEl.textContent = t('dashboard.errors.connection_error');
                return;
            }

            // Start polling every 5 seconds
            let pollCount = 0;
            const maxPolls = 120; // 120 * 5s = 10 minutes max
            const pollInterval = 5000;

            const pollTimer = setInterval(async () => {
                pollCount++;
                const elapsed = Math.round((Date.now() - startTime) / 1000);
                console.log(`🔍 Polling for data... (${pollCount}/${maxPolls}, ${elapsed}s elapsed)`);

                try {
                    const ready = await checkDataReady();
                    if (ready) {
                        console.log(`✅ Data ready after ${elapsed}s!`);
                        clearInterval(pollTimer);
                        clearInterval(messageInterval);

                        // Success state - show button, let user continue watching
                        if (titleEl) titleEl.textContent = t('dashboard.first_load.data_ready');
                        if (subtitleEl) subtitleEl.textContent = t('dashboard.first_load.data_ready_sub');
                        if (textEl) textEl.textContent = t('dashboard.first_load.completed_in', { seconds: elapsed });
                        if (progressBar) progressBar.style.width = '100%';
                        if (skipBtn) skipBtn.style.display = 'none';
                        if (readyBtn) {
                            readyBtn.style.display = 'inline-block';
                            readyBtn.onclick = () => {
                                // Mark welcome AND tutorial as seen (same 19min video)
                                localStorage.setItem('saas_welcome_seen_v1', Date.now().toString());
                                localStorage.setItem('saas_tutorial_clicked', Date.now().toString());
                                window.location.reload();
                            };
                        }
                        return;
                    }
                } catch (error) {
                    console.warn('⚠️ Poll check failed:', error);
                }

                // Timeout after max polls
                if (pollCount >= maxPolls) {
                    clearInterval(pollTimer);
                    clearInterval(messageInterval);
                    console.warn('⏱️ Polling timeout reached');

                    if (titleEl) titleEl.textContent = t('dashboard.first_load.taking_longer');
                    if (textEl) textEl.textContent = t('dashboard.first_load.process_continues');
                    if (progressBar) progressBar.style.width = '95%';
                    if (skipBtn) skipBtn.style.display = 'none';
                    // Show reload button for timeout case
                    if (readyBtn) {
                        readyBtn.textContent = t('dashboard.first_load.verify_again');
                        readyBtn.style.display = 'inline-block';
                        readyBtn.onclick = () => {
                            // Mark welcome AND tutorial as seen (same 19min video)
                            localStorage.setItem('saas_welcome_seen_v1', Date.now().toString());
                            localStorage.setItem('saas_tutorial_clicked', Date.now().toString());
                            window.location.reload();
                        };
                    }
                }
            }, pollInterval);
        }

        // Show error UI for auth/network errors
        function showErrorUI(errorMessage) {
            const mainContent = document.querySelector('.dashboard-grid');
            if (mainContent) {
                mainContent.innerHTML = `
                    <div style="grid-column: 1 / -1; text-align: center; padding: 60px 20px;">
                        <div style="font-size: 64px; margin-bottom: 20px;">❌</div>
                        <h2 style="color: #1d1d1f; margin-bottom: 16px;">${t('dashboard.errors.load_failed')}</h2>
                        <p style="color: #86868b; margin-bottom: 24px; max-width: 500px; margin-left: auto; margin-right: auto;">
                            ${errorMessage || t('dashboard.errors.could_not_load')}
                        </p>
                        <button onclick="window.location.reload()" style="
                            background: #007AFF; color: white; border: none; padding: 12px 24px;
                            border-radius: 8px; cursor: pointer; font-size: 14px;
                        ">${t('dashboard.errors.retry')}</button>
                    </div>
                `;
            }
        }
        
        async function updateDataDate() {
            // Use ACTUAL data date from metadata, not reference date
            const dateElement = document.getElementById('data-date');
            const comparisonDatesElement = document.getElementById('comparison-dates');
            
            if (dateElement) {
                try {
                    let displayText = t('dashboard.loading');
                    let actualDataDate = null;
                    let referenceDate;
                    
                    // PRIORITY 1: Try to get data_max_date from metadata (real data date)
                    if (window.optimizedData && window.optimizedData.meta && window.optimizedData.meta.metadata) {
                        actualDataDate = window.optimizedData.meta.metadata.data_max_date;
                        
                        if (actualDataDate) {
                            const [year, month, day] = actualDataDate.split('-');
                            displayText = `${day}/${month}/${year}`;
                            referenceDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day), 12, 0, 0);
                            
                            // Add hour if data is from today and we have reference_hour
                            const referenceHour = window.optimizedData.meta.metadata.reference_hour;
                            if (actualDataDate === window.optimizedData.meta.metadata.reference_date && referenceHour) {
                                const hourMatch = referenceHour.match(/(\d{2}):00:00$/);
                                if (hourMatch) {
                                    displayText += ` ${hourMatch[1]}h`;
                                }
                            }
                            
                            // Add a warning if data is old (use noon to avoid timezone shift)
                            const dataDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day), 12, 0, 0);
                            const today = new Date();
                            today.setHours(12, 0, 0, 0);
                            const daysDiff = Math.floor((today - dataDate) / (1000 * 60 * 60 * 24));
                            if (daysDiff > 1) {
                                displayText += ` ⚠️ (${daysDiff} días de retraso)`;
                            }
                        }
                    }
                    
                    // PRIORITY 2: Fallback to reference_date if no data_max_date
                    if (!actualDataDate) {
                        let referenceDateStr;
                        let referenceHour = null;
                        
                        // Try to get from optimized data metadata
                        if (window.optimizedData && window.optimizedData.meta && window.optimizedData.meta.metadata) {
                            referenceDateStr = window.optimizedData.meta.metadata.reference_date;
                            referenceHour = window.optimizedData.meta.metadata.reference_hour;
                        } 
                        // Fallback to periodsData
                        else if (window.periodsData && window.periodsData[7] && window.periodsData[7].metadata) {
                            referenceDateStr = window.periodsData[7].metadata.reference_date;
                            referenceHour = window.periodsData[7].metadata.reference_hour;
                        }
                        // Final fallback to yesterday
                        else {
                            const yesterday = new Date();
                            yesterday.setDate(yesterday.getDate() - 1);
                            referenceDateStr = yesterday.toISOString().split('T')[0];
                        }
                        
                        if (referenceDateStr) {
                            const [year, month, day] = referenceDateStr.split('-');
                            displayText = `${day}/${month}/${year}`;
                            referenceDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day), 12, 0, 0);
                            if (referenceHour) {
                                const hourMatch = referenceHour.match(/(\d{2}):00:00$/);
                                if (hourMatch) {
                                    // Convert from UTC to Mexico City time (UTC-6)
                                    let hour = parseInt(hourMatch[1]);
                                    hour = hour - 6; // Mexico City is UTC-6
                                    if (hour < 0) hour += 24;
                                    displayText += ` ${hour}h`;
                                }
                            }
                            displayText += ' (cargando datos...)';
                        }
                    }
                    
                    dateElement.textContent = displayText;
                    
                    // Update comparison dates if element exists
                    if (comparisonDatesElement) {
                        // Current week: 21-27 Aug (last 7 days)
                        const currentWeekEnd = new Date(referenceDate);
                        const currentWeekStart = new Date(referenceDate);
                        currentWeekStart.setDate(currentWeekStart.getDate() - 6);
                        
                        // Previous week: 14-20 Aug (7 days before that)
                        const prevWeekEnd = new Date(currentWeekStart);
                        prevWeekEnd.setDate(prevWeekEnd.getDate() - 1);
                        const prevWeekStart = new Date(prevWeekEnd);
                        prevWeekStart.setDate(prevWeekStart.getDate() - 6);
                        
                        const formatDate = (date) => {
                            return date.getDate() + ' ' + ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'][date.getMonth()];
                        };
                        
                        comparisonDatesElement.textContent = `Comparación entre semana actual (${formatDate(currentWeekStart)}-${formatDate(currentWeekEnd)}) vs semana anterior (${formatDate(prevWeekStart)}-${formatDate(prevWeekEnd)})`;
                    }
                } catch (error) {
                    // Fallback date
                    const yesterday = new Date();
                    yesterday.setDate(yesterday.getDate() - 1);
                    const day = String(yesterday.getDate()).padStart(2, '0');
                    const month = String(yesterday.getMonth() + 1).padStart(2, '0');
                    const year = yesterday.getFullYear();
                    dateElement.textContent = `${day}/${month}/${year}`;
                }
            }
        }
        
        // Make switchPeriod global for onclick handlers
        window.switchPeriod = function(button, days) {
            currentPeriod = days;
            
            // Vérifier si les données sont chargées
            if (!window.periodsData[days]) {
                console.log(`⏳ Datos ${days} días no cargados aún...`);
                button.textContent = `${days} días ⏳`;
                
                // Attendre que les données soient chargées
                const checkInterval = setInterval(() => {
                    if (window.periodsData[days]) {
                        clearInterval(checkInterval);
                        button.textContent = `${days} días`;
                        switchPeriod(button, days); // Réessayer
                    }
                }, 500); // Vérifier toutes les 500ms
                
                // Timeout après 30 secondes
                setTimeout(() => {
                    clearInterval(checkInterval);
                    button.textContent = `${days} días ❌`;
                }, 30000);
                return;
            }
            
            try {
                const n = window.periodsData[days] && window.periodsData[days].ads ? window.periodsData[days].ads.length : 0;
                console.log('🔄 Switching period ->', days, 'days; ads:', n);
            } catch (e) { console.warn('switchPeriod log error', e); }
            
            // ✅ Mettre à jour les boutons (corrigé)
            document.querySelectorAll('.period-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            button.classList.add('active');
            
            // Mettre à jour le contenu
            updateDashboard(days);
            
            // Auto-load demographics for new period
            if (currentAccountName && currentAccountName !== 'Todos') {
                setTimeout(() => loadDemographics(), 100);
            }
        }
        // Function switchAccount removed - was redirecting to non-existent petcare_real_analysis.html
        
        // ======================================================================
        // FONCTIONS DE RENDU POUR NOMENCLATURE DYNAMIQUE
        // ======================================================================
        
        function ensureNomenclatureSection() {
            let sec = document.getElementById('nomenclature-section');
            if (sec) return sec;

            sec = document.createElement('div');
            sec.id = 'nomenclature-section';
            sec.innerHTML = `
                <!-- ÁNGULOS CREATIVOS -->
                <div class="chart-card" style="margin-bottom: 30px;">
                    <h3>📊 Ángulos Creativos - ${currentAccountName}</h3>
                    <p style="color: #86868b; font-size: 14px; margin-bottom: 20px;">Análisis basado en la nomenclatura de los anuncios</p>
                    <p class="nomenclatura-note-angles" style="color:#ff9500;font-size:12px;"></p>
                    <div id="angles-container"></div>
                </div>
                
                <!-- PERFORMANCE POR CREADOR -->
                <div class="chart-card creators-card" style="margin-bottom: 30px;">
                    <h3>👥 Performance por Creador</h3>
                    <p style="color: #86868b; font-size: 14px; margin-bottom: 20px;">Análisis de rendimiento por creador de contenido</p>
                    <p class="nomenclatura-note-creators" style="color:#ff9500;font-size:12px;"></p>
                    <div id="creators-container"></div>
                </div>`;
            
            // Insérer APRÈS "Comparación Semana a Semana" et AVANT "Análisis Demográfico"
            // Chercher la section comparison (c'est la 2ème main-table-card)
            const allMainCards = document.querySelectorAll('.main-table-card');
            let insertAfter = null;
            
            // Trouver la carte "Comparación Semana a Semana"
            allMainCards.forEach(card => {
                const h2 = card.querySelector('h2');
                if (h2 && h2.textContent.includes('Comparación Semana a Semana')) {
                    insertAfter = card;
                }
            });
            
            if (insertAfter && insertAfter.nextSibling) {
                insertAfter.parentNode.insertBefore(sec, insertAfter.nextSibling);
            } else {
                // Fallback : insérer avant la section demographics ou à la fin
                const demoSection = document.querySelector('.preview-section');
                if (demoSection) {
                    demoSection.parentNode.insertBefore(sec, demoSection);
                } else {
                    document.querySelector('.container').appendChild(sec);
                }
            }
            return sec;
        }

        function renderAngles(map, container) {
            const data = [...map.entries()]
                .filter(([k]) => k) // garder même 'OTROS' mais on le met en bas
                .sort((a, b) => (b[1].spend - a[1].spend));

            const top = data.filter(([k]) => k !== 'OTROS').slice(0, 10); // Top 10 comme l'original
            const others = data.find(([k]) => k === 'OTROS');

            if (top.length === 0) {
                container.innerHTML = '<div style="color:#86868b;text-align:center;padding:20px;">Sin datos suficientes</div>';
                return;
            }

            const maxSpend = top[0]?.[1].spend || 1;
            container.innerHTML = `
                <div class="bar-chart" style="padding-bottom: 80px; position: relative;">
                    ${top.map(([k, m]) => {
                        const h = (m.spend / maxSpend) * 100;
                        const color = m.roas >= 2 ? '#00a854' : m.roas >= 1.2 ? '#ff9500' : '#ff3b30';
                        const icon = m.roas >= 2 ? '✅' : m.roas >= 1.2 ? '⚠️' : '❌';
                        return `
                            <div class="bar" style="height:${h}%; background: linear-gradient(135deg, ${color} 0%, ${color}99 100%); cursor:pointer"
                                 title="${k} • ${formatMoney(m.spend)} • ROAS ${m.roas.toFixed(2)}"
                                 data-angle="${escapeAttr(k)}">
                                <span class="bar-value">${formatMoney(m.spend / 1000)}k</span>
                                <span class="bar-roas" style="color:${color}">${m.roas.toFixed(1)} ${icon}</span>
                                <span class="bar-label">${k.length > 15 ? k.slice(0, 15) + '…' : k}</span>
                            </div>`;
                    }).join('')}
                </div>
                ${others ? `<div style="text-align:center;color:#86868b;font-size:12px;margin-top:8px;">
                    Otros ángulos (${data.length - top.length - 1} restantes): ${formatMoney(others[1].spend)} • ROAS ${others[1].roas.toFixed(2)}
                </div>` : ''}
            `;

            container.querySelector('.bar-chart')?.addEventListener('click', (e) => {
                const el = e.target.closest('[data-angle]'); 
                if (!el) return;
                const angle = el.getAttribute('data-angle');
                
                // Filtrer directement par le nom de l'annonce qui contient l'angle
                const nameFilter = document.querySelector('.table-filter[data-column="0"]');
                if (nameFilter) {
                    nameFilter.value = angle; 
                    window.applyTableFilters();
                    window.scrollTo({ 
                        top: document.querySelector('#ads-table').offsetTop - 80, 
                        behavior: 'smooth' 
                    });
                }
            });
        }

        function renderCreators(map, container) {
            // Toujours afficher les créateurs, même si peu de données

            const data = [...map.entries()]
                .filter(([k]) => k && k !== 'OTROS')
                .filter(([_, m]) => m.spend >= 500)  // Seuil plus bas pour voir plus de créateurs
                .sort((a, b) => b[1].roas - a[1].roas)
                .slice(0, 9); // Top 9 créateurs

            if (data.length === 0) { 
                container.innerHTML = `<div style="color:#86868b;text-align:center;padding:20px;">
                    Sin creadores identificados
                </div>`; 
                return; 
            }

            container.innerHTML = `
                <div class="creator-grid">
                    ${data.map(([name, m]) => {
                        // Déterminer l'avatar et la classe selon le nom
                        let avatarClass = '';
                        let avatar = '👤';
                        let avatarStyle = 'background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);';
                        
                        if (/melissa|priscilla|karla|sam|esme|za|abril|karime|deni|aranza/i.test(name)) {
                            avatar = '👩';
                            avatarClass = 'female';
                            avatarStyle = ''; // Utiliser le style de la classe CSS
                        } else if (/martin|leo|dany/i.test(name)) {
                            avatar = '👨';
                            avatarClass = 'male';
                            avatarStyle = ''; // Utiliser le style de la classe CSS
                        } else if (/voz ia|ia|ai/i.test(name)) {
                            avatar = '🤖';
                        }
                        
                        return `
                            <div class="creator-card" data-creator="${escapeAttr(name)}" style="cursor:pointer">
                                <div class="creator-avatar ${avatarClass}" ${avatarStyle ? `style="${avatarStyle}"` : ''}>
                                    ${avatar}
                                </div>
                                <div class="creator-name">${name}</div>
                                <div class="creator-stats">
                                    ROAS: <span class="creator-roas">${m.roas.toFixed(1)}x</span><br>
                                    ${m.ads} ads • ${formatMoney(m.spend / 1000)}k
                                </div>
                            </div>`;
                    }).join('')}
                </div>
            `;

            container.querySelector('.creator-grid')?.addEventListener('click', (e) => {
                const el = e.target.closest('[data-creator]'); 
                if (!el) return;
                const creator = el.getAttribute('data-creator');
                
                // Filtrer par nom d'annonce contenant le créateur
                const nameFilter = document.querySelector('.table-filter[data-column="0"]');
                if (nameFilter) {
                    nameFilter.value = creator; 
                    window.applyTableFilters();
                    window.scrollTo({ 
                        top: document.querySelector('#ads-table').offsetTop - 80, 
                        behavior: 'smooth' 
                    });
                }
            });
        }

        function updateNomenclatureSections(data) {
            const ads = data.ads || [];
            const getOv = (id) => (JSON.parse(localStorage.getItem('nomenclatureOverrides') || '{}'))[id];
            const { angle, creator, score, coverage } = buildIndexes(ads, getOv);

            const section = ensureNomenclatureSection();
            const noteAngles = section.querySelector('.nomenclatura-note-angles');
            const noteCreators = section.querySelector('.nomenclatura-note-creators');
            
            // Mettre à jour le nom du compte dans le titre
            const angleTitle = section.querySelector('.chart-card h3');
            if (angleTitle) {
                angleTitle.textContent = `📊 Ángulos Creativos - ${currentAccountName}`;
            }

            // Masquer pour "Todos" car trop de comptes mélangés
            if (currentAccountName === 'Todos') {
                section.style.display = 'none';
                return;
            }

            if (score < 40) {
                section.style.display = 'block';
                // Show contextual alert instead of empty charts
                showNomenclatureAlert(section, coverage);
                return;
            }

            noteAngles.textContent = '';
            noteCreators.textContent = '';
            section.style.display = 'block';
            renderAngles(angle, section.querySelector('#angles-container'));
            
            // Toujours afficher les créateurs, même avec une couverture faible
            renderCreators(creator, section.querySelector('#creators-container'));
            if (score < 70) {
                noteCreators.innerHTML = `ℹ️ Análisis parcial (coverage ${coverage.toFixed(0)}%)`;
            }
        }
        
        // === Fonctions pour les graphiques Tipo de Hook ===
        function ensureHookSection() {
            let grid = document.getElementById('hook-section');
            if (grid) return grid;
            
            grid = document.createElement('div');
            grid.id = 'hook-section';
            grid.className = 'chart-grid';
            grid.style.marginTop = '20px';
            
            grid.innerHTML = `
                <div class="chart-card">
                    <h3>🔖 Tipo de Hook — % de participación (anuncios activos)</h3>
                    <div id="hook-share-chart" class="bar-chart" style="padding-bottom: 90px;"></div>
                    <p id="hook-share-note" style="text-align:center;color:#86868b;margin-top:8px;font-size:12px;"></p>
                </div>
                <div class="chart-card">
                    <h3>🛒 Tipo de Hook — % por compras</h3>
                    <div id="hook-purchase-chart" class="bar-chart" style="padding-bottom: 90px;"></div>
                    <p id="hook-purchase-note" style="text-align:center;color:#86868b;margin-top:8px;font-size:12px;"></p>
                </div>
            `;
            
            // On l'insère "en bas" : après la dernière section de .container
            const container = document.querySelector('.container');
            container.appendChild(grid);
            
            return grid;
        }
        
        // Calcule les stats hooks sur les ads du compte courant (agrégées)
        function computeHookStats(ads) {
            // Base = annonces "actives" = spend > 0
            const base = ads.filter(a => parseFloat(a.spend || 0) > 0);
            
            // Compteurs
            const counts = {};
            const purchases = {};
            HOOK_OPTIONS.forEach(o => { counts[o] = 0; purchases[o] = 0; });
            
            // Récup localStorage (cache déjà maintenu)
            base.forEach(ad => {
                const saved = savedHypothesesCache.get(ad.ad_id);
                const hook = (saved?.hookType) || '';
                if (!hook) return; // non assigné -> compté dans "non assignés" pour la note uniquement
                if (!counts.hasOwnProperty(hook)) return; // au cas où une ancienne valeur incohérente existe
                counts[hook] += 1;
                purchases[hook] += (ad.purchases || 0);
            });
            
            const totalActive = base.length;
            const assigned = Object.values(counts).reduce((s, v) => s + v, 0);
            const unassigned = totalActive - assigned;
            const totalPurchasesAssigned = Object.values(purchases).reduce((s, v) => s + v, 0);
            
            return { counts, purchases, totalActive, assigned, unassigned, totalPurchasesAssigned };
        }
        
        function renderHookCharts(stats) {
            // Helper function to get shorter labels for charts
            const getShortLabel = (label) => {
                if (label === 'Exclamaciones / Provocaciones') return 'Provocaciones';
                if (label === 'Listas o Pasos') return 'Listas';
                return label;
            };
            
            // 1) % de participation (sur tous les actifs) = counts / totalActive
            const shareEl = document.getElementById('hook-share-chart');
            if (shareEl) {
                const maxPct = stats.totalActive > 0 ? Math.max(...HOOK_OPTIONS.map(o => (stats.counts[o] / stats.totalActive) * 100)) : 0;
                shareEl.innerHTML = HOOK_OPTIONS.map(o => {
                    const pct = stats.totalActive > 0 ? (stats.counts[o] / stats.totalActive) * 100 : 0;
                    const h = maxPct > 0 ? (pct / maxPct) * 100 : 5;
                    return `
                        <div class="bar" style="height:${h}%">
                            <span class="bar-value">${pct.toFixed(0)}%</span>
                            <span class="bar-label" title="${o}">${getShortLabel(o)}</span>
                        </div>
                    `;
                }).join('');
                const note = document.getElementById('hook-share-note');
                if (note) {
                    const cover = stats.totalActive > 0 ? (stats.assigned / stats.totalActive * 100).toFixed(0) : '0';
                    note.textContent = `Base: ${stats.totalActive} anuncios activos • Asignados: ${stats.assigned} (${cover}%) • Sin asignar: ${stats.unassigned}`;
                }
            }
            
            // 2) % por compras (répartition des purchases entre hooks assignés)
            const purchEl = document.getElementById('hook-purchase-chart');
            if (purchEl) {
                if (stats.totalPurchasesAssigned <= 0) {
                    purchEl.innerHTML = `<div style="color:#86868b;text-align:center;width:100%;">Sin compras en anuncios con Tipo de Hook asignado</div>`;
                } else {
                    const maxPct = Math.max(...HOOK_OPTIONS.map(o => (stats.purchases[o] / stats.totalPurchasesAssigned) * 100));
                    purchEl.innerHTML = HOOK_OPTIONS.map(o => {
                        const pct = (stats.purchases[o] / stats.totalPurchasesAssigned) * 100;
                        const h = maxPct > 0 ? (pct / maxPct) * 100 : 5;
                        return `
                            <div class="bar" style="height:${h}%">
                                <span class="bar-value">${pct.toFixed(0)}%</span>
                                <span class="bar-label" title="${o}">${getShortLabel(o)}</span>
                            </div>
                        `;
                    }).join('');
                }
                const note = document.getElementById('hook-purchase-note');
                if (note) {
                    note.textContent = stats.totalPurchasesAssigned > 0
                        ? `Base: ${stats.totalPurchasesAssigned} compras (solo anuncios con Tipo de Hook asignado)`
                        : `Base: 0 compras`;
                }
            }
        }

        // ========== NUEVO: Grid de "Iteraciones vs Nuevos" + "Awareness" ==========

        function ensureInsightsSection() {
          let grid = document.getElementById('insights-grid');
          if (grid) return grid;

          grid = document.createElement('div');
          grid.id = 'insights-grid';
          grid.className = 'chart-grid';
          grid.style.marginTop = '20px';
          grid.innerHTML = `
            <div class="chart-card fixed-height">
              <h3>
                % Iteraciones vs Creativos Nuevos
                <select id="iter-metric" style="margin-left: 10px; padding: 4px 8px; border-radius: 6px; border: 1px solid #d1d1d6; font-size: 12px;">
                  <option value="spend" selected>Por Gastos</option>
                  <option value="count">Por Anuncios</option>
                  <option value="purchases">Por Compras</option>
                </select>
              </h3>
              <div id="iter-vs-new-chart" class="bar-chart" style="padding-bottom: 90px;"></div>
              <p id="iter-note" style="text-align:center;color:#86868b;margin-top:8px;font-size:12px;"></p>
            </div>

            <div class="chart-card fixed-height">
              <h3>
                🧠 Awareness Levels — Distribución
                <select id="awareness-metric" style="margin-left: 10px; padding: 4px 8px; border-radius: 6px; border: 1px solid #d1d1d6; font-size: 12px;">
                  <option value="spend" selected>Por Gastos</option>
                  <option value="count">Por Anuncios</option>
                  <option value="purchases">Por Compras</option>
                </select>
              </h3>
              <div id="awareness-chart" class="bar-chart" style="padding-bottom: 90px;"></div>
              <p id="awareness-note" style="text-align:center;color:#86868b;margin-top:8px;font-size:12px;"></p>
            </div>
          `;

          // Insérer juste avant la première "main-table-card"
          const container = document.querySelector('.container');
          const firstTableCard = document.querySelector('.main-table-card');
          if (container && firstTableCard) container.insertBefore(grid, firstTableCard);
          else if (container) container.appendChild(grid);

          // Listeners (re-rendu quand on change de base)
          grid.querySelector('#iter-metric').addEventListener('change', () => {
            if (window.currentData) updateIterationVsNewSection(window.currentData);
          });
          grid.querySelector('#awareness-metric').addEventListener('change', () => {
            if (window.currentData) updateAwarenessSection(window.currentData);
          });

          return grid;
        }

        function computeTypeStats(ads) {
          // base = anuncios activos (spend > 0) — cohérent avec vos KPI
          const base = (ads || []).filter(a => parseFloat(a.spend || 0) > 0);

          const sums = {
            nuevo: { spend: 0, count: 0, purchases: 0 },
            iter:  { spend: 0, count: 0, purchases: 0 },
            other: { spend: 0, count: 0, purchases: 0 }
          };

          base.forEach(ad => {
            const t = (getAdType(ad.ad_name) || '').toLowerCase(); // 'nuevo' | 'iteración' | '—'
            const spend = parseFloat(ad.spend || 0);
            const purchases = parseInt(ad.purchases || 0, 10);

            if (t.startsWith('iter')) {
              sums.iter.spend += spend; sums.iter.count += 1; sums.iter.purchases += purchases;
            } else if (t.startsWith('nuevo')) {
              sums.nuevo.spend += spend; sums.nuevo.count += 1; sums.nuevo.purchases += purchases;
            } else {
              sums.other.spend += spend; sums.other.count += 1; sums.other.purchases += purchases;
            }
          });

          const totals = {
            spend:     sums.nuevo.spend + sums.iter.spend,
            count:     sums.nuevo.count + sums.iter.count,
            purchases: sums.nuevo.purchases + sums.iter.purchases
          };

          return { baseCount: base.length, sums, totals };
        }

        function updateIterationVsNewSection(data) {
          ensureInsightsSection();
          const ads = data.ads || [];
          const metricSel = document.getElementById('iter-metric');
          const metric = metricSel ? metricSel.value : 'spend';

          const stats = computeTypeStats(ads);
          const denom = stats.totals[metric] || 0;

          const chart = document.getElementById('iter-vs-new-chart');
          const note  = document.getElementById('iter-note');

          if (!chart) return;

          if (denom <= 0) {
            chart.innerHTML = `<div style="color:#86868b;text-align:center;width:100%;">Sin datos suficientes</div>`;
            if (note) note.textContent = '';
            return;
          }

          const nuevoVal = stats.sums.nuevo[metric];
          const iterVal  = stats.sums.iter[metric];
          const nuevoPct = (nuevoVal / denom) * 100;
          const iterPct  = (iterVal  / denom) * 100;

          // Couverture (part "válida" vs total incluant "otros")
          const totalWithOthers = stats.totals[metric] + stats.sums.other[metric];
          const coveragePct = totalWithOthers > 0 ? (stats.totals[metric] / totalWithOthers) * 100 : 0;

          chart.innerHTML = `
            <div class="bar" style="height:${nuevoPct.toFixed(1)}%; background: linear-gradient(135deg, #06b6d4 0%, #0891b2 100%);">
              <span class="bar-value">${nuevoPct.toFixed(0)}%</span>
              <span class="bar-label">Nuevo</span>
            </div>
            <div class="bar" style="height:${iterPct.toFixed(1)}%; background: linear-gradient(135deg, #7c3aed 0%, #5b21b6 100%);">
              <span class="bar-value">${iterPct.toFixed(0)}%</span>
              <span class="bar-label">Iteración</span>
            </div>
          `;

          if (note) {
            let baseText = '';
            if (metric === 'spend')      baseText = `${formatMoney(denom)} gastados`;
            else if (metric === 'count') baseText = `${denom} anuncios`;
            else                         baseText = `${denom} compras`;

            note.textContent = `Base: ${baseText} • Cobertura "Tipo" válido: ${coveragePct.toFixed(0)}%`;
          }
        }

        const AWARENESS_LEVELS = ['Unaware', 'Problem', 'Solution', 'Product', 'Most Aware'];

        function computeAwarenessStats(ads) {
          const base = (ads || []).filter(a => parseFloat(a.spend || 0) > 0);

          const sums = {
            'Unaware':     { spend: 0, count: 0, purchases: 0 },
            'Problem':     { spend: 0, count: 0, purchases: 0 },
            'Solution':    { spend: 0, count: 0, purchases: 0 },
            'Product':     { spend: 0, count: 0, purchases: 0 },
            'Most Aware':  { spend: 0, count: 0, purchases: 0 },
            'Sin etiqueta':{ spend: 0, count: 0, purchases: 0 }
          };

          base.forEach(ad => {
            const lvl = getAwarenessLevel(ad.ad_name, ad.ad_id) || '—';
            const key = AWARENESS_LEVELS.includes(lvl) ? lvl : 'Sin etiqueta';
            const spend = parseFloat(ad.spend || 0);
            const purchases = parseInt(ad.purchases || 0, 10);

            sums[key].spend += spend;
            sums[key].count += 1;
            sums[key].purchases += purchases;
          });

          const totals = {
            spend:     Object.values(sums).reduce((s, v) => s + v.spend, 0),
            count:     base.length,
            purchases: Object.values(sums).reduce((s, v) => s + v.purchases, 0)
          };

          const assignedSpend     = AWARENESS_LEVELS.reduce((s, k) => s + sums[k].spend, 0);
          const assignedCount     = AWARENESS_LEVELS.reduce((s, k) => s + sums[k].count, 0);
          const assignedPurchases = AWARENESS_LEVELS.reduce((s, k) => s + sums[k].purchases, 0);

          return { sums, totals, assigned: { spend: assignedSpend, count: assignedCount, purchases: assignedPurchases } };
        }

        function updateAwarenessSection(data) {
          ensureInsightsSection();
          const ads = data.ads || [];
          const metricSel = document.getElementById('awareness-metric');
          const metric = metricSel ? metricSel.value : 'spend';

          const stats = computeAwarenessStats(ads);
          const denom = stats.totals[metric] || 0;

          const chart = document.getElementById('awareness-chart');
          const note  = document.getElementById('awareness-note');
          if (!chart) return;

          if (denom <= 0) {
            chart.innerHTML = `<div style="color:#86868b;text-align:center;width:100%;">Sin datos suficientes</div>`;
            if (note) note.textContent = '';
            return;
          }

          const config = [
            { key: 'Unaware',      label: 'Unaware',     color: '#7dd3fc' },
            { key: 'Problem',      label: 'Problem',     color: '#60a5fa' },
            { key: 'Solution',     label: 'Solution',    color: '#3b82f6' },
            { key: 'Product',      label: 'Product',     color: '#2563eb' },
            { key: 'Most Aware',   label: 'Most',        color: '#1e40af' },
            { key: 'Sin etiqueta', label: 'Sin etiqueta',color: '#94a3b8' }
          ];

          chart.innerHTML = config.map(({key, label, color}) => {
            const val = stats.sums[key][metric];
            const pct = denom > 0 ? (val / denom) * 100 : 0;
            return `
              <div class="bar" style="height:${pct.toFixed(1)}%; background: linear-gradient(135deg, ${color} 0%, ${color}CC 100%);">
                <span class="bar-value">${pct.toFixed(0)}%</span>
                <span class="bar-label" title="${label}">${label}</span>
              </div>
            `;
          }).join('');

          if (note) {
            const assignedPct = denom > 0 ? (stats.assigned[metric] / denom) * 100 : 0;
            let baseText = '';
            if (metric === 'spend')      baseText = `${formatMoney(denom)} gastados`;
            else if (metric === 'count') baseText = `${denom} anuncios`;
            else                         baseText = `${denom} compras`;

            note.textContent = `Base: ${baseText} • Asignados: ${assignedPct.toFixed(0)}%`;
          }
        }

        function updateHookSection(data) {
            // crée le conteneur si besoin
            ensureHookSection();
            // on calcule à partir des ads agrégées + filtre compte courant déjà appliqué en amont
            const ads = filterAdsByAccount(data.ads || []);
            const stats = computeHookStats(ads);
            renderHookCharts(stats);
        }

        function showNoDataMessage(days) {
            // Masquer KPIs (afficher 0)
            const kpiCards = document.querySelectorAll('.kpi-content');
            if (kpiCards.length >= 5) {
                kpiCards[0].innerHTML = '<h3>0</h3><p>Anuncios con Inversión</p>';
                const currCode = getCurrentCurrency();
                kpiCards[1].innerHTML = `<h3>${formatMoney(0)}</h3><p>${currCode ? `Inversión Total (${currCode})` : 'Inversión Total'}</p>`;
                kpiCards[2].innerHTML = '<h3>—</h3><p>ROAS Promedio</p>';
                kpiCards[3].innerHTML = '<h3>—</h3><p>Valor de Conversión</p>';
                kpiCards[4].innerHTML = '<h3>—</h3><p>CPA Promedio</p>';
            }

            // Masquer/vider les graphiques principaux
            const barChart = document.querySelector('.bar-chart');
            if (barChart) {
                barChart.innerHTML = '<div style="text-align:center;color:#86868b;padding:40px;">Sin datos para este período</div>';
            }

            // Vider le tableau principal
            const tableBody = document.querySelector('#ads-table tbody');
            if (tableBody) {
                tableBody.innerHTML = `
                    <tr>
                        <td colspan="16" style="text-align:center;padding:40px;color:#86868b;">
                            📊 Sin datos para ${currentAccountName} en los últimos ${days} días
                        </td>
                    </tr>
                `;
            }

            // Vider le tableau de formats
            const formatTable = document.getElementById('formato-table');
            if (formatTable) formatTable.innerHTML = '';

            // Vider la comparaison
            const comparisonTable = document.getElementById('comparison-table');
            if (comparisonTable) comparisonTable.innerHTML = '';

            // Masquer les sections nomenclature/hooks/insights
            const nomenclatureSection = document.getElementById('nomenclature-section');
            if (nomenclatureSection) nomenclatureSection.style.display = 'none';

            const hookSection = document.getElementById('hook-section');
            if (hookSection) hookSection.style.display = 'none';

            const insightsGrid = document.getElementById('insights-grid');
            if (insightsGrid) insightsGrid.style.display = 'none';
        }

        function updateDashboard(days) {
            // Load period data on demand if not already loaded
            let data = window.periodsData[days];
            if (!data && window.loadPeriodData) {
                console.log(`⏳ Loading ${days}d data on demand...`);
                data = window.loadPeriodData(days);
                window.periodsData[days] = data;
            }
            
            if (!data) {
                console.error(`No hay datos para ${days} días`);
                return;
            }
            console.log('✅ Rendering period', days, 'ads:', data.ads ? data.ads.length : 'n/a');
            
            // NOUVEAU: Filtrer AVANT d'agréger pour garder les bonnes métriques
            const filteredByAccount = filterAdsByAccount(data.ads || []);
            const originalCount = filteredByAccount.length;
            const aggregatedAds = aggregateAdsByAdId(filteredByAccount);
            console.log(`📊 Agrégation pour ${currentAccountName}: ${originalCount} lignes journalières → ${aggregatedAds.length} ads uniques`);
            
            // Maintenant viewAds = aggregatedAds (déjà filtrés)
            const viewAds = aggregatedAds;

            // 🆕 FIX UX: Si aucun ad, afficher message "Sin datos" au lieu de spinner infini
            if (viewAds.length === 0) {
                console.warn(`⚠️ Sin datos para ${days} días (cuenta: ${currentAccountName})`);
                showNoDataMessage(days);
                return;
            }

            const viewData = {
                ...data,
                ads: viewAds,
                format_distribution: computeFormatDistribution(viewAds),
                originalAdsCount: originalCount  // Passer le nombre original
            };

            updateKPIs(viewData);
            updateCharts(viewData);
            updateTable(viewData);
            updateFormatTable(viewData);
            updateComparisonTable();
            
            // NOUVEAU: Mise à jour des sections de nomenclature dynamique
            updateNomenclatureSections(viewData);

            // 🆕 graphiques Insights (Iteraciones vs Nuevos + Awareness)
            ensureInsightsSection();
            updateIterationVsNewSection(viewData);
            updateAwarenessSection(viewData);

            // 🆕 graphiques Tipo de Hook (en bas)
            updateHookSection(viewData);
            
            // Afficher/masquer la section Petcare statique (ancien code)
            const petcareSection = document.getElementById('petcare-section');
            if (petcareSection) {
                petcareSection.style.display = 'none'; // Toujours masquer l'ancien code statique
            }
        }
        
        function updateKPIs(data) {
            const ads = data.ads || [];
            const any = ads[0];
            const currentAccId = any ? (any.account_id || '') : '';
            const profile = getAccountProfile(currentAccId); // 'leads' or 'ecom'

            // Un ad est "actif sur la période" s'il a eu du spend (donc il tournait)
            const activeAds = ads.filter(ad => parseFloat(ad.spend || 0) > 0).length;
            const totalAds = ads.length;
            // Convertir spend de string à number
            const totalSpend = ads.reduce((sum, ad) => sum + parseFloat(ad.spend || 0), 0);
            const totalRevenue = ads.reduce((sum, ad) => sum + (ad.purchase_value || 0), 0);
            const totalPurchases = ads.reduce((sum, ad) => sum + (ad.purchases || 0), 0);
            const avgRoas = totalSpend > 0 ? (totalRevenue / totalSpend) : 0;
            const cpa = totalPurchases > 0 ? (totalSpend / totalPurchases) : 0;

            // Leads-specific aggregates
            const totalImpr   = ads.reduce((s,a)=> s + (a.impressions||0), 0);
            const totalReachA = ads.reduce((s,a)=> s + (a.reach||0), 0); // proxy using max reach per ad
            const totalUClk   = ads.reduce((s,a)=> s + (a.unique_link_clicks||0), 0);
            const totalLeads  = ads.reduce((s,a)=> s + (a.results||0), 0);
            const cpm         = totalImpr > 0 ? (totalSpend / totalImpr * 1000) : 0;
            const ctrUnique   = (totalReachA > 0 && totalUClk > 0) ? (totalUClk / totalReachA * 100) : 0; // using reach proxy

            // Mettre à jour les valeurs dans les KPI cards (5 cartes désormais)
            const kpiCards = document.querySelectorAll('.kpi-content');
            if (kpiCards.length >= 5) {
                // KPI 1: Anuncios con Inversión (ceux qui ont eu du spend) - unchanged
                kpiCards[0].innerHTML = `
                    <h3>${activeAds.toLocaleString()}</h3>
                    <p>${t('dashboard.kpis.ads_with_spend')}</p>
                `;

                // KPI 2: Total Spend (with dynamic currency)
                const currencyCode = getCurrentCurrency();
                const spendLabel = currencyCode ? `Inversión Total (${currencyCode})` : t('dashboard.kpis.total_spend');
                kpiCards[1].innerHTML = `
                    <h3>${formatMoney(totalSpend)}</h3>
                    <p>${spendLabel}</p>
                `;

                if (profile === 'leads') {
                    // KPI 3 → CTR único
                    const v3 = document.getElementById('kpi-roas-value');
                    const l3 = document.getElementById('kpi-roas-label');
                    if (v3) v3.textContent = (ctrUnique > 0 ? ctrUnique.toFixed(2) + '%' : 'N/A');
                    if (l3) l3.textContent = t('dashboard.kpis.unique_ctr');

                    // KPI 4 → CPM
                    const v4 = document.getElementById('kpi-extra-value');
                    const l4 = document.getElementById('kpi-extra-label');
                    if (v4) v4.textContent = (cpm > 0 ? formatMoney(cpm, {decimals: 2}) : 'N/A');
                    if (l4) l4.textContent = t('dashboard.kpis.cpm');

                    // KPI 5 → Costo por Resultado
                    const v5 = document.getElementById('kpi-cpa-value');
                    const l5 = document.getElementById('kpi-cpa-label');
                    const cpr = totalLeads > 0 ? (totalSpend / totalLeads) : 0;
                    if (v5) v5.textContent = (cpr > 0 ? formatMoney(cpr, {decimals: 2}) : 'N/A');
                    if (l5) l5.textContent = t('dashboard.kpis.cost_per_result');
                } else {
                    // ecom defaults
                    const v3 = document.getElementById('kpi-roas-value');
                    const l3 = document.getElementById('kpi-roas-label');
                    if (v3) v3.textContent = avgRoas.toFixed(2);
                    if (l3) l3.textContent = t('dashboard.kpis.avg_roas');

                    const v4 = document.getElementById('kpi-extra-value');
                    const l4 = document.getElementById('kpi-extra-label');
                    if (v4) v4.textContent = formatMoney(totalRevenue);
                    if (l4) l4.textContent = t('dashboard.kpis.conversion_value');

                    const v5 = document.getElementById('kpi-cpa-value');
                    const l5 = document.getElementById('kpi-cpa-label');
                    if (v5) v5.textContent = cpa > 0 ? formatMoney(cpa, {decimals: 2}) : 'N/A';
                    if (l5) l5.textContent = t('dashboard.kpis.avg_cpa');
                }
            }
        }
        
        // Function updateNuevoIteracionStats removed - was updating non-existent DOM elements
        
        function updateCharts(data) {
            // Store data globally for reuse
            window.currentData = data;
            const ads = data.ads || [];
            
            // Removed call to updateNuevoIteracionStats - elements don't exist
            
            // Changer le titre et le contenu selon la sélection
            const chartTitle = document.querySelector('.chart-card h3');
            const barChart = document.querySelector('.bar-chart');
            
            if (currentAccountName === 'Todos') {
                // Mode TODOS: Top 5 comptes par spend
                if (chartTitle) chartTitle.textContent = t('dashboard.charts.top_accounts');
                
                const accountSpend = {};
                ads.forEach(ad => {
                    if (!accountSpend[ad.account_name]) accountSpend[ad.account_name] = 0;
                    accountSpend[ad.account_name] += parseFloat(ad.spend || 0);
                });
                
                const topAccounts = Object.entries(accountSpend)
                    .sort((a, b) => b[1] - a[1])
                    .slice(0, 5);
                
                const maxSpend = topAccounts[0] ? topAccounts[0][1] : 1;
                
                if (barChart) {
                    barChart.innerHTML = topAccounts.map(([name, spend]) => `
                        <div class="bar" style="height: ${(spend/maxSpend*100)}%">
                            <span class="bar-value">${formatMoney(spend/1000, {accountName: name})}k</span>
                            <span class="bar-label">${escapeHtml(name.substring(0, 10))}</span>
                        </div>
                    `).join('');
                }
            } else {
                // Mode COMPTE SPÉCIFIQUE: Top 5 anuncios
                const sortBy = window.topSortBy || 'purchases'; // Default to purchases (ventas)
                const displayMetric = sortBy === 'purchases' ? t('dashboard.charts.by_sales') : t('dashboard.charts.by_spend');
                if (chartTitle) {
                    chartTitle.innerHTML = `
                        ${t('dashboard.charts.top_ads')} ${escapeHtml(currentAccountName.substring(0, 20))}
                        <select id="top5-sort" style="margin-left: 10px; padding: 4px 8px; border-radius: 6px; border: 1px solid #d1d1d6; font-size: 12px;" onchange="window.topSortBy = this.value; if(window.currentData) updateCharts(window.currentData);">
                            <option value="spend" ${sortBy === 'spend' ? 'selected' : ''}>${t('dashboard.charts.by_spend')}</option>
                            <option value="purchases" ${sortBy === 'purchases' ? 'selected' : ''}>${t('dashboard.charts.by_sales')}</option>
                        </select>
                    `;
                }
                
                // Filtrer et trier les anuncios (sortBy already declared above)
                const topAds = ads
                    .filter(ad => {
                        if (sortBy === 'purchases') {
                            return parseInt(ad.purchases || 0) > 0;
                        }
                        return parseFloat(ad.spend || 0) > 0;
                    })
                    .sort((a, b) => {
                        if (sortBy === 'purchases') {
                            return parseInt(b.purchases || 0) - parseInt(a.purchases || 0);
                        }
                        return parseFloat(b.spend || 0) - parseFloat(a.spend || 0);
                    })
                    .slice(0, 5);
                
                const maxValue = topAds[0] ? (sortBy === 'purchases' ? parseInt(topAds[0].purchases) : parseFloat(topAds[0].spend)) : 1;
                
                if (barChart) {
                    // Debug: log media URLs to check what's available
                    console.log('Top 5 ads media check:');
                    topAds.forEach(ad => {
                        console.log('Ad:', ad.ad_name?.substring(0, 30), 
                                    '- media_url:', ad.media_url, 
                                    '- media:', ad.media, 
                                    '- video_url:', ad.video_url, 
                                    '- image_url:', ad.image_url);
                    });
                    
                    barChart.innerHTML = topAds.map(ad => {
                        const value = sortBy === 'purchases' ? parseInt(ad.purchases || 0) : parseFloat(ad.spend || 0);
                        const displayValue = sortBy === 'purchases' ?
                            `${value} ventas` :
                            `${formatMoney(value/1000, {accountName: ad.account_name})}k`;
                        const roasColor = ad.roas >= 2.0 ? '#00a854' : ad.roas >= 1.2 ? '#ff9500' : '#ff3b30';
                        const roasClass = roasColor; // pour garder votre dégradé
                        const barHeight = (value / maxValue * 100);
                        
                        const mediaUrl = getMediaUrl(ad);
                        const clickable = !!mediaUrl;
                        const cursorStyle = clickable ? 'cursor: pointer;' : '';
                        const icon = ad.roas >= 2.0 ? '✅' : ad.roas >= 1.2 ? '⚠️' : '❌';
                        
                        return `
                            <div class="bar" 
                                 style="height:${barHeight}%; background: linear-gradient(135deg, ${roasClass} 0%, ${roasClass}99 100%); ${cursorStyle}"
                                 data-media-url="${escapeAttr(mediaUrl)}"
                                 title="${clickable ? 'Click para ver media' : 'Sin media disponible'}">
                                <span class="bar-value">${displayValue}</span>
                                <span class="bar-roas" style="color:${roasColor}">ROAS ${ad.roas.toFixed(1)} ${icon}</span>
                                <span class="bar-label" title="${escapeHtml(ad.ad_name)}">${escapeHtml((ad.ad_name||'').substring(0,10))}</span>
                            </div>
                        `;
                    }).join('');
                    
                    // Add click handler (not additive) for bars with media URLs
                    barChart.onclick = (e) => {
                        const el = e.target.closest('.bar[data-media-url]');
                        if (!el) return;
                        const url = el.getAttribute('data-media-url');
                        if (!url) return;
                        try {
                            window.open(url, '_blank', 'noopener');
                        } catch (err) {
                            console.error('Open failed', err);
                        }
                    };
                }
            }
            
            // Mettre à jour le donut chart ET la légende des formats avec ROAS
            const formatStats = data.format_distribution || {};
            const formatRoas = calculateFormatRoas(ads);
            
            // Ne pas filtrer - montrer TOUS les formats pour transparence
            const filteredFormats = Object.entries(formatStats);
            
            // Calculer le total avec TOUS les formats
            const total = filteredFormats.reduce((sum, [_, count]) => sum + count, 0);
            const circumference = 2 * Math.PI * 60; // rayon = 60
            
            // Générer le SVG dynamiquement
            const donutSvg = document.querySelector('.donut-svg');
            if (donutSvg && total > 0) {
                let offset = 0;
                let segments = '';
                let labels = '';

                filteredFormats.forEach(([format, count]) => {
                    const pct = count / total;
                    const dashLength = circumference * pct;
                    const currentOffset = offset;
                    offset += dashLength;

                    // Segment
                    segments += `<circle cx="90" cy="90" r="60"
                                    fill="none"
                                    stroke="${getFormatColor(format)}"
                                    stroke-width="30"
                                    stroke-dasharray="${dashLength} ${circumference}"
                                    stroke-dashoffset="-${currentOffset}"
                                    transform="rotate(-90 90 90)"></circle>`;

                    // Label % (au centre de l'arc). Masquer <5% pour éviter le chevauchement
                    const pctInt = Math.round(pct * 100);
                    if (pctInt >= 5) {
                        const mid = currentOffset + dashLength / 2;
                        const angle = (mid / circumference) * 2 * Math.PI - Math.PI / 2; // -90°
                        const rLabel = 60; // rayon milieu de l'anneau (stroke=30 => milieu à r=60)
                        const x = 90 + Math.cos(angle) * rLabel;
                        const y = 90 + Math.sin(angle) * rLabel;
                        labels += `<text x="${x.toFixed(1)}" y="${y.toFixed(1)}"
                                       text-anchor="middle" dominant-baseline="middle"
                                       font-size="13" font-weight="700" fill="white"
                                       stroke="#1d1d1f" stroke-width="0.5">${pctInt}%</text>`;
                    }
                });

                donutSvg.innerHTML = segments + '<circle cx="90" cy="90" r="30" fill="white"></circle>' + labels;
            }
            
            // Légende: ajouter aussi le pourcentage
            const legend = document.querySelector('.legend');
            if (legend) {
                legend.innerHTML = filteredFormats
                    .map(([fmt, count]) => {
                        const roas = formatRoas[fmt] || 0;
                        const pct = Math.round((count / total) * 100);
                        // Renommer UNKNOWN en OTROS pour cohérence avec le tableau
                        const displayName = fmt === 'UNKNOWN' ? 'OTROS' : fmt;
                        const shortName = fmt === 'INSTAGRAM' ? 'IG' : displayName;
                        return `
                            <div class="legend-item">
                                <div class="legend-color" style="background: ${getFormatColor(fmt)}"></div>
                                <span>${shortName} (${count})${roas > 0 ? ` • ROAS:${roas.toFixed(2)}` : ''}</span>
                            </div>
                        `;
                    }).join('');
            }
        }
        
        function calculateFormatRoas(ads) {
            const formatMetrics = {};
            
            ads.forEach(ad => {
                const fmt = ad.format;
                if (!formatMetrics[fmt]) {
                    formatMetrics[fmt] = { totalSpend: 0, totalRevenue: 0 };
                }
                formatMetrics[fmt].totalSpend += parseFloat(ad.spend || 0);
                formatMetrics[fmt].totalRevenue += ad.purchase_value;
            });
            
            // ✅ ROAS pondéré par spend (correct)
            const formatRoas = {};
            Object.keys(formatMetrics).forEach(fmt => {
                const { totalSpend, totalRevenue } = formatMetrics[fmt];
                formatRoas[fmt] = totalSpend > 0 ? totalRevenue / totalSpend : 0;
            });
            
            return formatRoas;
        }
        
        function getFormatColor(format) {
            const colors = {
                'VIDEO': '#0ea5e9',      // Bleu clair
                'IMAGE': '#2563eb',      // Bleu moyen
                'INSTAGRAM': '#1e40af',  // Bleu foncé
                'UNKNOWN': '#94a3b8',    // Gris-bleu
                'CAROUSEL': '#06b6d4'    // Cyan
            };
            return colors[format] || '#ccc';
        }
        
        // Stocker les données actuelles pour le filtrage
        let currentTableData = [];
        let filteredTableData = [];
        
        // Variables de pagination
        let currentPage = 1;
        // Pagination: mémoriser le choix utilisateur (10/25/50/100)
        let itemsPerPage = parseInt(localStorage.getItem('adsPageSize') || '10', 10);
        
        // État du tri: par défaut "plus récent d'abord" (Fecha Inicio)
        let sortState = JSON.parse(localStorage.getItem('adsSort') || 'null') || { key: 'date', dir: 'desc' };
        
        // Helpers pour le tri
        function num(v){ return parseFloat(v || 0); }
        function cmp(a,b){ return a<b ? -1 : a>b ? 1 : 0; }
        function parseDateSafe(d){ return d ? new Date(d).getTime() : 0; }

        // Applique le tri courant à un tableau d'annonces
        function applySort(arr){
          const dir = sortState.dir === 'asc' ? 1 : -1;
          const key = sortState.key;
          return [...arr].sort((a,b)=>{
            switch(key){
              case 'spend':      return dir * (num(a.spend) - num(b.spend));
              case 'roas':       return dir * ((a.roas || 0) - (b.roas || 0));
              case 'cpa':        return dir * ((a.cpa  || 0) - (b.cpa  || 0));
              case 'purchases':  return dir * ((a.purchases || 0) - (b.purchases || 0));
              case 'date':       return dir * (parseDateSafe(a.created_time) - parseDateSafe(b.created_time));
              case 'ad':
              default:           return dir * cmp((a.ad_name||'').toLowerCase(), (b.ad_name||'').toLowerCase());
            }
          });
        }

        // Installe les listeners de tri sur l'en-tête de table
        function initTableSorting(){
          const headRow = document.querySelector('#ads-table thead tr:first-child');
          if(!headRow) return;
          const map = [
            { idx:0, key:'ad',        label:'Anuncio' },
            { idx:3, key:'date',      label:'Fecha Inicio' },
            { idx:4, key:'spend',     label:'Importe Gastado' },
            { idx:5, key:'roas',      label:'ROAS' },
            { idx:6, key:'cpa',       label:'CPA' },
            { idx:7, key:'purchases', label:'Compras' }
          ];

          map.forEach(({idx,key,label})=>{
            const th = headRow.children[idx];
            if(!th) return;
            th.classList.add('sortable');
            th.setAttribute('data-sort-key', key);
            th.addEventListener('click', ()=>{
              if (sortState.key === key) { sortState.dir = (sortState.dir === 'asc' ? 'desc' : 'asc'); }
              else { sortState = { key, dir:'desc' }; }
              localStorage.setItem('adsSort', JSON.stringify(sortState));
              currentPage = 1;
              renderTable();
              renderSortIndicators(headRow, map);
            });
          });
          renderSortIndicators(headRow, map);
        }

        function renderSortIndicators(headRow, map){
          map.forEach(({idx,key,label})=>{
            const th = headRow.children[idx];
            if (!th) return;
            if (sortState.key === key) {
              th.innerHTML = `${label} <span class="sort-ind">${sortState.dir === 'asc' ? '▲' : '▼'}</span>`;
            } else {
              th.innerHTML = label;
            }
          });
        }
        
        // Helper functions to extract from nomenclature
        function getAwarenessLevel(adName, adId) {
            // 1) Priorité aux overrides manuels sauvegardés
            const savedData = JSON.parse(localStorage.getItem('adHypotheses') || '{}');
            if (adId && savedData[adId] && savedData[adId].awarenessLevel) {
                return savedData[adId].awarenessLevel;
            }
            
            // 2) Détection automatique depuis le nom
            if (!adName) return '—';
            const name = adName.toLowerCase();
            
            // NEW: Unaware (qq synonymes fréquents)
            if (name.includes('unaware') || /\bcold\b/.test(name) || /\btofu\b/.test(name) || name.includes('top of funnel')) {
                return 'Unaware';
            }
            
            if (name.includes('most aware') || name.includes('mostaware')) return 'Most Aware';
            if (name.includes('problem')) return 'Problem';
            if (name.includes('solution')) return 'Solution';
            if (name.includes('product')) return 'Product';
            
            // 3) Cas "nomenclature_avec_underscores"
            const parts = adName.split('_');
            if (parts.length > 3) {
                const raw = (parts[3] || '').toLowerCase().replace(/[^\w]/g, ''); // normalise
                if (raw === 'unaware' || raw === 'cold' || raw === 'tofu') return 'Unaware';
                if (raw.startsWith('most')) return 'Most Aware';
                if (raw === 'problem') return 'Problem';
                if (raw === 'solution') return 'Solution';
                if (raw === 'product') return 'Product';
            }
            return '—';
        }
        
        // Cache pour éviter de parser plusieurs fois le même nom
        const parseCache = new Map();
        
        function parseOnce(adName) {
            if (!parseCache.has(adName)) {
                parseCache.set(adName, window.NOMEN_V2.parseAdName(adName));
            }
            return parseCache.get(adName);
        }
        
        // Clear cache when data changes
        window.clearParseCache = function() {
            parseCache.clear();
        };
        
        function getAdType(adName) {
            const parsed = parseOnce(adName);
            return parsed.type || '—';
        }
        
        function getAngle(adName) {
            if (!adName) return '—';
            const parsed = parseOnce(adName);
            return parsed.angle || '—';
        }
        
        
        
        
        
        
        
        function updateTable(data) {
            const ads = data.ads || [];
            // Clear parse cache when data changes
            if (window.clearParseCache) window.clearParseCache();
            // Par défaut: tri par "Fecha Inicio" (plus récent → plus ancien)
            currentTableData = applySort(ads);
            filteredTableData = currentTableData; // Par défaut, pas de filtre
            currentPage = 1; // Réinitialiser à la première page
            
            // Populate angle filter dynamically
            populateAngleFilter(ads);
            
            // Appliquer les filtres si nécessaire
            const spendFilter = document.querySelector('.table-filter[data-column="4"]');
            if (spendFilter && spendFilter.value) {
                // Appliquer le filtre après avoir défini currentTableData
                setTimeout(() => {
                    window.applyTableFilters();
                }, 100);
            } else {
                renderTable();
            }
        }
        
        function populateAngleFilter(ads) {
            const angleFilter = document.querySelector('.table-filter[data-column="10"]');
            if (!angleFilter) return;
            
            // Collect unique angles from ads
            const angles = new Set();
            ads.forEach(ad => {
                const angle = getAngle(ad.ad_name);
                if (angle && angle !== '—') {
                    angles.add(angle);
                }
            });
            
            // Sort angles alphabetically
            const sortedAngles = Array.from(angles).sort();
            
            // Keep current selection if exists
            const currentValue = angleFilter.value;
            
            // Clear and rebuild options
            angleFilter.innerHTML = '<option value="">Todos</option>';
            sortedAngles.forEach(angle => {
                const option = document.createElement('option');
                option.value = angle.toLowerCase();
                option.textContent = angle;
                angleFilter.appendChild(option);
            });
            
            // Restore previous selection if it still exists
            if (currentValue) {
                angleFilter.value = currentValue;
            }
        }
        
        function renderTable() {
            // Mettre à jour le titre de la table
            const tableTitle = document.getElementById('table-title');
            if (tableTitle) {
                tableTitle.textContent = t('dashboard.table.title', { period: currentPeriod });
            }
            
            // Appliquer le tri aux données filtrées
            const sortedData = applySort(filteredTableData);
            
            // Calculer la pagination
            const totalPages = Math.ceil(sortedData.length / itemsPerPage);
            const startIndex = (currentPage - 1) * itemsPerPage;
            const endIndex = startIndex + itemsPerPage;
            const pageData = sortedData.slice(startIndex, endIndex);
            
            const tableSubtitle = document.querySelector('.main-table-card p');
            if (tableSubtitle && !tableSubtitle.hasAttribute('data-i18n')) {
                const filterInfo = filteredTableData.length < currentTableData.length
                    ? ` (${filteredTableData.length} ${t('dashboard.table.pagination.filtered')} ${currentTableData.length})`
                    : '';
                tableSubtitle.textContent = t('dashboard.table.pagination.showing', {
                    start: startIndex + 1,
                    end: Math.min(endIndex, sortedData.length),
                    total: sortedData.length.toLocaleString()
                }) + filterInfo;
            }
            
            // Charger les données sauvegardées depuis localStorage
            const savedData = JSON.parse(localStorage.getItem('adHypotheses') || '{}');
            
            // Mettre à jour le tbody
            const tableBody = document.querySelector('#ads-table tbody');
            if (tableBody) {
                tableBody.innerHTML = pageData.map(ad => {
                    const roasClass = ad.roas >= ROAS_THRESHOLDS.HIGH ? 'roas-high' : ad.roas >= ROAS_THRESHOLDS.MEDIUM ? 'roas-medium' : 'roas-low';
                    const formatClass = `format-${ad.format.toLowerCase()}`;
                    
                    let mediaLink = '—';
                    if (ad.media_url && !ad.media_url.toLowerCase().startsWith('javascript:')) {
                        const icon = ad.format === 'VIDEO' ? '🎬' : '🖼️';
                        const safeUrl = ad.media_url.replace(/['"]/g, '');
                        // Tooltip discret pour Instagram (pas d'alert intrusive)
                        const title = safeUrl.includes('instagram.com') ? 
                            ' title="Instagram carousel - connexion requise si \'Post unavailable\'"' : '';
                        mediaLink = `<a href="${safeUrl}" target="_blank" rel="noopener noreferrer" class="media-link"${title}>${icon}</a>`;
                    }
                    
                    // Déterminer le badge d'état
                    const statusBadge = ad.effective_status === 'ACTIVE' 
                        ? '<span class="status-badge active">ACTIVO</span>'
                        : '<span class="status-badge paused">PAUSADO</span>';
                    
                    // Formater la date de début
                    const startDate = ad.created_time ? new Date(ad.created_time).toLocaleDateString('es-MX', {
                        day: '2-digit',
                        month: '2-digit',
                        year: '2-digit'
                    }) : '—';
                    
                    // Récupérer les valeurs sauvegardées
                    const savedAd = savedData[ad.ad_id] || {};
                    const hypothesis = savedAd.hypothesis || '';
                    const learning = savedAd.learning || '';
                    const hookType = savedAd.hookType || '';
                    const result = savedAd.result || '';
                    
                    return `
                        <tr data-ad-id="${ad.ad_id}">
                            <td class="ad-name" title="${escapeHtml(ad.ad_name)} (ID: ${ad.ad_id})">
                                ${escapeHtml(ad.ad_name)}
                                <span style="color: #999; font-size: 11px; display: block; margin-top: 2px;">ID: ${ad.ad_id.slice(-8)}</span>
                            </td>
                            <td>${statusBadge}</td>
                            <td><span class="format-badge ${formatClass}">${ad.format}</span></td>
                            <td>${startDate}</td>
                            <td class="metric">${formatMoney(parseFloat(ad.spend || 0), {accountName: ad.account_name})}</td>
                            <td class="metric ${roasClass}">${ad.roas.toFixed(2)}</td>
                            <td class="metric">${ad.cpa > 0 ? formatMoney(ad.cpa, {accountName: ad.account_name}) : '—'}</td>
                            <td class="metric">${ad.purchases || 0}</td>
                            <td style="text-align: center;">
                                <select class="awareness-select" data-ad-id="${ad.ad_id}">
                                    <option value="">—</option>
                                    <option value="Unaware" ${getAwarenessLevel(ad.ad_name, ad.ad_id) === 'Unaware' ? 'selected' : ''}>Unaware</option>
                                    <option value="Problem" ${getAwarenessLevel(ad.ad_name, ad.ad_id) === 'Problem' ? 'selected' : ''}>Problem</option>
                                    <option value="Solution" ${getAwarenessLevel(ad.ad_name, ad.ad_id) === 'Solution' ? 'selected' : ''}>Solution</option>
                                    <option value="Product" ${getAwarenessLevel(ad.ad_name, ad.ad_id) === 'Product' ? 'selected' : ''}>Product</option>
                                    <option value="Most Aware" ${getAwarenessLevel(ad.ad_name, ad.ad_id) === 'Most Aware' ? 'selected' : ''}>Most Aware</option>
                                </select>
                            </td>
                            <td style="text-align: center;">${getAdType(ad.ad_name)}</td>
                            <td style="text-align: center;">${getAngle(ad.ad_name)}</td>
                            <td style="text-align: center;">
                                <select class="hook-select" data-ad-id="${ad.ad_id}">
                                    <option value="">—</option>
                                    ${HOOK_OPTIONS.map(opt => `
                                        <option value="${opt}" ${hookType === opt ? 'selected' : ''}>${opt}</option>
                                    `).join('')}
                                </select>
                            </td>
                            <td>${mediaLink}</td>
                            <td>
                                <div class="hypothesis-wrapper">
                                    <textarea class="hypothesis-input" placeholder="${t('dashboard.table.filters.hypothesis_placeholder')}" data-ad-id="${ad.ad_id}"
                                        rows="1"
                                        style="height: 36px;"
                                        onkeyup="this.style.height='36px'; this.style.height = Math.min(this.scrollHeight, 100) + 'px';"
                                        onfocus="this.style.height = Math.min(this.scrollHeight, 100) + 'px';"
                                        onload="this.style.height = Math.min(this.scrollHeight, 100) + 'px';">${hypothesis}</textarea>
                                </div>
                            </td>
                            <td>
                                <div class="hypothesis-wrapper">
                                    <textarea class="learning-input" placeholder="${t('dashboard.table.filters.learnings_placeholder')}" data-ad-id="${ad.ad_id}"
                                        rows="1"
                                        style="height: 36px;"
                                        onkeyup="this.style.height='36px'; this.style.height = Math.min(this.scrollHeight, 100) + 'px';"
                                        onfocus="this.style.height = Math.min(this.scrollHeight, 100) + 'px';"
                                        onload="this.style.height = Math.min(this.scrollHeight, 100) + 'px';">${learning}</textarea>
                                </div>
                            </td>
                            <td>
                                <select class="result-select" data-ad-id="${ad.ad_id}">
                                    <option value="">—</option>
                                    <option value="ganador" ${result === 'ganador' ? 'selected' : ''}>✅ Ganador</option>
                                    <option value="perdedor" ${result === 'perdedor' ? 'selected' : ''}>❌ Perdedor</option>
                                </select>
                            </td>
                        </tr>
                    `;
                }).join('');
            }
            
            // Ajouter les contrôles de pagination
            renderPaginationControls(totalPages);
        }
        
        function renderPaginationControls(totalPages) {
            let paginationContainer = document.querySelector('.pagination-controls');
            if (!paginationContainer) {
                paginationContainer = document.createElement('div');
                paginationContainer.className = 'pagination-controls';
                paginationContainer.style.cssText = `
                    display:flex; justify-content:center; align-items:center; gap:10px; margin-top:20px; padding:15px; flex-wrap:wrap;
                `;
                const tableCard = document.querySelector('.main-table-card');
                if (tableCard) tableCard.appendChild(paginationContainer);
            }

            paginationContainer.innerHTML = `
                <div class="page-size">
                    ${t('dashboard.table.pagination.rows')}
                    <select id="page-size">
                        <option value="10">10</option>
                        <option value="25">25</option>
                        <option value="50">50</option>
                        <option value="100">100</option>
                    </select>
                </div>
                <button onclick="changePage(1)" ${currentPage === 1 ? 'disabled' : ''} style="padding: 8px 12px; border: 1px solid #e0e0e2; background: white; border-radius: 6px; ${currentPage === 1 ? 'opacity:0.5; cursor:not-allowed;' : 'cursor:pointer;'}">←</button>
                <button onclick="changePage(${currentPage - 1})" ${currentPage === 1 ? 'disabled' : ''} style="padding: 8px 12px; border: 1px solid #e0e0e2; background: white; border-radius: 6px; ${currentPage === 1 ? 'opacity:0.5; cursor:not-allowed;' : 'cursor:pointer;'}">${t('dashboard.table.pagination.previous')}</button>
                <span style="padding: 8px 16px; background: #f5f5f7; border-radius: 6px;">${t('dashboard.table.pagination.page')} <strong>${currentPage}</strong> ${t('dashboard.table.pagination.of')} <strong>${totalPages}</strong></span>
                <button onclick="changePage(${currentPage + 1})" ${currentPage === totalPages ? 'disabled' : ''} style="padding: 8px 12px; border: 1px solid #e0e0e2; background: white; border-radius: 6px; ${currentPage === totalPages ? 'opacity:0.5; cursor:not-allowed;' : 'cursor:pointer;'}">${t('dashboard.table.pagination.next')}</button>
                <button onclick="changePage(${totalPages})" ${currentPage === totalPages ? 'disabled' : ''} style="padding: 8px 12px; border: 1px solid #e0e0e2; background: white; border-radius: 6px; ${currentPage === totalPages ? 'opacity:0.5; cursor:not-allowed;' : 'cursor:pointer;'}">→</button>
            `;

            // Appliquer la valeur et écouter les changements
            const select = paginationContainer.querySelector('#page-size');
            if (select) {
                select.value = String(itemsPerPage);
                select.addEventListener('change', (e)=>{
                    itemsPerPage = parseInt(e.target.value, 10);
                    localStorage.setItem('adsPageSize', String(itemsPerPage));
                    currentPage = 1;
                    renderTable();
                });
            }
        }
        
        function changePage(page) {
            const totalPages = Math.ceil(filteredTableData.length / itemsPerPage);
            if (page >= 1 && page <= totalPages) {
                currentPage = page;
                renderTable();
            }
        }
        
        // Fonction de filtrage unifiée avec cache localStorage
        let savedHypothesesCache = new Map();
        function refreshHypothesesCache() {
            try {
                savedHypothesesCache = new Map(
                    Object.entries(JSON.parse(localStorage.getItem('adHypotheses') || '{}'))
                );
            } catch {
                savedHypothesesCache = new Map();
            }
        }
        refreshHypothesesCache();
        window.addEventListener('storage', e => { if (e.key === 'adHypotheses') refreshHypothesesCache(); });
        
        window.applyTableFilters = function() {
            const filters = {};
            document.querySelectorAll('.table-filter').forEach(el => {
                filters[+el.dataset.column] = String(el.value || '').toLowerCase().trim();
            });

            filteredTableData = currentTableData.filter(ad => {
                // 0: nom
                if (filters[0] && !ad.ad_name.toLowerCase().includes(filters[0])) return false;

                // 1: état ('active' / 'paused')
                if (filters[1]) {
                    const status = String(ad.effective_status || '').toLowerCase();
                    if (filters[1] === 'active' && status !== 'active') return false;
                    if (filters[1] === 'paused' && status === 'active') return false;
                }

                // 2: format exact en minuscules
                if (filters[2] && String(ad.format || '').toLowerCase() !== filters[2]) return false;

                // 3: date (égalité jour)
                if (filters[3] && ad.created_time) {
                    const adD = new Date(ad.created_time); adD.setHours(0,0,0,0);
                    const fD  = new Date(filters[3]);      fD.setHours(0,0,0,0);
                    if (adD.getTime() !== fD.getTime()) return false;
                }

                // 4: dépense min
                if (filters[4] && parseFloat(ad.spend || 0) < parseFloat(filters[4])) return false;

                // 5: ROAS min
                if (filters[5] && (ad.roas || 0) < parseFloat(filters[5])) return false;

                // 6: CPA max
                if (filters[6] && (ad.cpa || 0) > parseFloat(filters[6])) return false;

                // 7: achats min
                if (filters[7] && (ad.purchases || 0) < parseInt(filters[7])) return false;
                
                // 8: awareness level
                if (filters[8]) {
                    const awareness = getAwarenessLevel(ad.ad_name, ad.ad_id).toLowerCase();
                    if (filters[8] === 'most' && !awareness.includes('most')) return false;
                    if (filters[8] !== 'most' && !awareness.includes(filters[8])) return false;
                }
                
                // 9: tipo (nuevo/iteración)
                if (filters[9]) {
                    const tipo = getAdType(ad.ad_name).toLowerCase();
                    if (filters[9] === 'nuevo' && tipo !== 'nuevo') return false;
                    if (filters[9] === 'iteracion' && tipo !== 'iteración') return false;
                }
                
                // 10: ángulo
                if (filters[10]) {
                    const angle = getAngle(ad.ad_name).toLowerCase();
                    if (angle !== filters[10]) return false;
                }
                
                // 11: tipo de hook (basé sur le localStorage)
                if (filters[11]) {
                    const hook = (savedHypothesesCache.get(ad.ad_id)?.hookType || '').toLowerCase();
                    if (hook !== filters[11]) return false;
                }
                
                // 12: Ver (no filter)
                // 13: Hipótesis (no filter)
                // 14: Aprendizajes (no filter)
                
                // 15: resultado (ganador/perdedor)
                if (filters[15]) {
                    const res = savedHypothesesCache.get(ad.ad_id)?.result || '';
                    if (res !== filters[15]) return false;
                }

                return true;
            });

            currentPage = 1;
            renderTable();
        }
        
        function updateFormatTable(data) {
            const ads = data.ads || [];
            
            // Calculer métriques par format
            const formatMetrics = {};
            
            ads.forEach(ad => {
                const fmt = ad.format;
                if (!formatMetrics[fmt]) {
                    formatMetrics[fmt] = {
                        count: 0,
                        totalSpend: 0,
                        totalRevenue: 0,
                        totalCtr: 0,
                        totalPurchases: 0,
                        validCtr: 0
                    };
                }
                
                formatMetrics[fmt].count += 1;
                formatMetrics[fmt].totalSpend += parseFloat(ad.spend || 0);
                formatMetrics[fmt].totalRevenue += ad.purchase_value || 0;
                formatMetrics[fmt].totalPurchases += (ad.purchases || 0);
                
                const ctr = (parseFloat(ad.clicks || 0) / parseFloat(ad.impressions || 1)) * 100;
                if (ctr > 0) {
                    formatMetrics[fmt].totalCtr += ctr;
                    formatMetrics[fmt].validCtr += 1;
                }
            });
            
            // Remplir le tableau - Utiliser le même calcul ROAS que le donut
            const formatTable = document.getElementById('formato-table');
            if (formatTable) {
                formatTable.innerHTML = Object.entries(formatMetrics)
                    // Ne pas filtrer - montrer TOUS les formats
                    .sort((a, b) => b[1].totalSpend - a[1].totalSpend)
                    .map(([fmt, metrics]) => {
                        // ROAS pondéré (comme dans le donut)
                        const avgRoas = metrics.totalSpend > 0 ? (metrics.totalRevenue / metrics.totalSpend) : 0;
                        const avgCtr = metrics.validCtr > 0 ? (metrics.totalCtr / metrics.validCtr) : 0;
                        const cpa = metrics.totalPurchases > 0 ? (metrics.totalSpend / metrics.totalPurchases) : 0;
                        
                        const formatClass = `format-${fmt.toLowerCase()}`;
                        const displayFormat = fmt === 'UNKNOWN' ? 'OTROS' : fmt;
                        
                        return `
                            <tr>
                                <td><span class="format-badge ${formatClass}">${displayFormat}</span></td>
                                <td class="metric">${metrics.count.toLocaleString()}</td>
                                <td class="metric">${formatMoney(metrics.totalSpend)}</td>
                                <td class="metric">${avgRoas.toFixed(2)}</td>
                                <td class="metric">${cpa > 0 ? formatMoney(cpa, {decimals: 2}) : 'N/A'}</td>
                                <td class="metric">${metrics.totalPurchases.toLocaleString()}</td>
                                <td>${avgCtr.toFixed(2)}%</td>
                            </tr>
                        `;
                    }).join('');
            }
        }
        
        // Expose globally for async update
        window.updateComparisonTable = function updateComparisonTable() {
            // ALWAYS use 7-day data for weekly comparison, regardless of selected period
            const currentData = window.periodsData[7];  // Always compare last 7 days
            const prevData = window.prevWeekData || prevWeekData;  // Check global variable
            
            // Vérifier que les données sont disponibles
            if (!currentData || !currentData.ads) {
                console.warn('Datos actuales no disponibles');
                return;
            }
            
            // Si pas de données de comparaison, afficher N/A
            if (!prevData || !prevData.ads || prevData.ads.length === 0) {
                const comparisonTable = document.getElementById('comparison-table');
                if (comparisonTable) {
                    comparisonTable.innerHTML = '<tr><td colspan="4" style="text-align: center;">Datos de comparación no disponibles</td></tr>';
                }
                return;
            }
            
            // Filter by account if needed
            const current = filterAdsByAccount(currentData.ads || []);
            const previous = filterAdsByAccount(prevData.ads || []);
            
            // Check if we have data after filtering
            if (current.length === 0 || previous.length === 0) {
                const comparisonTable = document.getElementById('comparison-table');
                if (comparisonTable) {
                    comparisonTable.innerHTML = '<tr><td colspan="4" style="text-align: center;">No hay datos para esta cuenta</td></tr>';
                }
                return;
            }
            
            // ✅ Fonction d'agrégation pondérée (correcte)
            const aggregate = (ads) => {
                const totalSpend = ads.reduce((sum, ad) => sum + parseFloat(ad.spend || 0), 0);
                const totalRevenue = ads.reduce((sum, ad) => sum + ad.purchase_value, 0);
                const totalPurchases = ads.reduce((sum, ad) => sum + (ad.purchases || 0), 0);
                
                return {
                    spend: totalSpend,
                    roas: totalSpend > 0 ? totalRevenue / totalSpend : 0,
                    cpa: totalPurchases > 0 ? totalSpend / totalPurchases : 0
                };
            };
            
            const currentMetrics = aggregate(current);
            const prevMetrics = aggregate(previous);
            
            // Calculer changements
            const spendChange = ((currentMetrics.spend - prevMetrics.spend) / prevMetrics.spend * 100);
            const roasChange = ((currentMetrics.roas - prevMetrics.roas) / prevMetrics.roas * 100);
            const cpaChange = prevMetrics.cpa > 0 ? ((currentMetrics.cpa - prevMetrics.cpa) / prevMetrics.cpa * 100) : 0;
            const adsChange = ((current.length - previous.length) / previous.length * 100);
            
            function formatChange(change) {
                // Gérer les cas spéciaux
                if (!isFinite(change)) {
                    // Quand on passe de 0 à quelque chose (division par zéro)
                    return `<span style="color: #6c757d; cursor: help;" title="Sin datos en la semana anterior para comparar">N/A</span>`;
                }
                if (Math.abs(change) > 999) {
                    // Pour les changements extrêmes mais calculables
                    return `<span style="color: #00a854; font-weight: 600;" title="Cambio superior al 999%">+999%+</span>`;
                }
                const symbol = change > 0 ? '+' : '';
                const color = change > 0 ? '#00a854' : change < 0 ? '#ff3b30' : '#6c757d';
                return `<span style="color: ${color}; font-weight: 600;">${symbol}${change.toFixed(1)}%</span>`;
            }
            
            const comparisonTable = document.getElementById('comparison-table');
            if (comparisonTable) {
                comparisonTable.innerHTML = `
                    <tr>
                        <td class="metric">Anuncios con Inversión</td>
                        <td class="metric">${current.length.toLocaleString()}</td>
                        <td class="metric">${previous.length.toLocaleString()}</td>
                        <td>${formatChange(adsChange)}</td>
                    </tr>
                    <tr>
                        <td class="metric">Inversión Total</td>
                        <td class="metric">${formatMoney(currentMetrics.spend)}</td>
                        <td class="metric">${formatMoney(prevMetrics.spend)}</td>
                        <td>${formatChange(spendChange)}</td>
                    </tr>
                    <tr>
                        <td class="metric">ROAS Promedio</td>
                        <td class="metric">${currentMetrics.roas.toFixed(2)}</td>
                        <td class="metric">${prevMetrics.roas.toFixed(2)}</td>
                        <td>${formatChange(roasChange)}</td>
                    </tr>
                    <tr>
                        <td class="metric">CPA Promedio</td>
                        <td class="metric">${currentMetrics.cpa > 0 ? formatMoney(currentMetrics.cpa, {decimals: 2}) : 'N/A'}</td>
                        <td class="metric">${prevMetrics.cpa > 0 ? formatMoney(prevMetrics.cpa, {decimals: 2}) : 'N/A'}</td>
                        <td>${prevMetrics.cpa > 0 ? formatChange(cpaChange) : 'N/A'}</td>
                    </tr>
                `;
            }
        }

        // NOUVELLE FONCTION: Agréger les données journalières par ad_id
        function aggregateAdsByAdId(ads) {
            if (!ads || ads.length === 0) return [];
            
            const aggregated = {};
            
            ads.forEach(ad => {
                const id = ad.ad_id;
                
                if (!aggregated[id]) {
                    // Première occurrence - copier toutes les métadonnées
                    aggregated[id] = { ...ad };
                    // S'assurer que les valeurs numériques sont des nombres
                    aggregated[id].spend = parseFloat(ad.spend || 0);
                    aggregated[id].impressions = parseFloat(ad.impressions || 0);
                    aggregated[id].clicks = parseFloat(ad.clicks || 0);
                    aggregated[id].purchases = ad.purchases || 0;
                    aggregated[id].purchase_value = ad.purchase_value || 0;
                } else {
                    // Occurrences suivantes - additionner les métriques
                    aggregated[id].spend += parseFloat(ad.spend || 0);
                    aggregated[id].impressions += parseFloat(ad.impressions || 0);
                    aggregated[id].clicks += parseFloat(ad.clicks || 0);
                    aggregated[id].purchases += (ad.purchases || 0);
                    aggregated[id].purchase_value += (ad.purchase_value || 0);
                    
                    // Garder la date de début la plus ancienne
                    if (ad.created_time && ad.created_time < aggregated[id].created_time) {
                        aggregated[id].created_time = ad.created_time;
                    }
                }
            });
            
            // Recalculer les métriques dérivées
            Object.values(aggregated).forEach(ad => {
                // Recalculer ROAS avec les totaux agrégés
                ad.roas = ad.spend > 0 ? (ad.purchase_value / ad.spend) : 0;
                
                // Recalculer CPA avec les totaux agrégés
                ad.cpa = ad.purchases > 0 ? (ad.spend / ad.purchases) : 0;
                
                // Recalculer CTR avec les totaux agrégés
                ad.ctr = ad.impressions > 0 ? ((ad.clicks / ad.impressions) * 100) : 0;
                
                // S'assurer que spend est une string pour la compatibilité
                ad.spend = ad.spend.toString();
            });
            
            return Object.values(aggregated);
        }

        // Helpers de filtrage/distribution
        function filterAdsByAccount(ads) {
            if (!ads) return [];
            // Make sure we handle all cases properly
            if (currentAccountName === 'Todos' || currentAccountName === 'Todas las cuentas' || currentAccountName === 'global' || !currentAccountName) {
                return ads; // Return all ads for "Todas las cuentas"
            }
            return ads.filter(a => a.account_name === currentAccountName);
        }
        function computeFormatDistribution(ads) {
            const dist = {};
            ads.forEach(a => { const f = a.format || 'UNKNOWN'; dist[f] = (dist[f]||0)+1; });
            return dist;
        }
        function buildAccountOptions() {
            const menu = document.getElementById('account-dropdown-menu');
            if (!menu) return;

            // Get accounts that have at least 1 ad in the last 90 days
            const accountsWithData = new Set();
            const data90 = window.periodsData[90];
            if (data90 && data90.ads && data90.ads.length > 0) {
                data90.ads.forEach(a => {
                    if (a.account_name) accountsWithData.add(a.account_name);
                });
            }

            let names = [];
            if (accountsWithData.size > 0) {
                // Filter to only show accounts with data in the last 90 days
                if (accountsIndex && Array.isArray(accountsIndex.accounts)) {
                    names = accountsIndex.accounts
                        .map(a => a.name)
                        .filter(name => name && accountsWithData.has(name))
                        .sort();
                } else {
                    names = Array.from(accountsWithData).sort();
                }
                console.log(`📋 Dropdown: ${names.length} cuentas con datos (de ${accountsIndex?.accounts?.length || '?'} totales)`);
            } else if (accountsIndex && Array.isArray(accountsIndex.accounts)) {
                // Fallback: data not loaded yet, show all accounts from index
                names = accountsIndex.accounts.map(a => a.name).filter(Boolean).sort();
                console.log(`📋 Dropdown: mostrando ${names.length} cuentas (datos aún no cargados)`);
            }

            let optionsHTML = `<div class="dropdown-option ${currentAccountName === 'Todos' ? 'active' : ''}" data-value="Todos">${t('dashboard.accounts.all')}</div>`;
            names.forEach(n => {
                const displayName = n.length > 28 ? n.substring(0, 25) + '...' : n;
                optionsHTML += `<div class="dropdown-option ${currentAccountName === n ? 'active' : ''}" data-value="${n}" title="${n}">${displayName}</div>`;
            });

            // Injecter la barre de recherche + options
            menu.innerHTML = `
              <div class="dropdown-search">
                <input id="account-search" type="text" placeholder="${t('dashboard.accounts.search')}">
              </div>
              ${optionsHTML}
            `;

            // Listeners pour options
            menu.querySelectorAll('.dropdown-option').forEach(option => {
                option.addEventListener('click', (e) => {
                    const value = e.currentTarget.getAttribute('data-value');
                    selectAccount(value);
                    closeDropdown();
                });
            });

            // Filtre en direct
            const searchInput = document.getElementById('account-search');
            if (searchInput) {
                const filterOptions = () => {
                    const q = searchInput.value.toLowerCase().trim();
                    menu.querySelectorAll('.dropdown-option').forEach(opt => {
                        const text = (opt.getAttribute('title') || opt.textContent || '').toLowerCase();
                        opt.style.display = !q || text.includes(q) ? 'block' : 'none';
                    });
                };
                searchInput.addEventListener('input', filterOptions);
                // Focus auto quand le dropdown s'ouvre
                setTimeout(()=> searchInput.focus(), 0);
            }
        }
        
        function selectAccount(value) {
            currentAccountName = value;
            const btnText = document.getElementById('account-dropdown-text');
            if (btnText) {
                btnText.textContent = value === 'Todos' ? t('dashboard.accounts.all') :
                                     (value.length > 28 ? value.substring(0, 25) + '...' : value);
            }
            
            // Gérer le bouton de partage
            updateShareButton();
            
            // Show demographics section for all accounts
            const demographicsSection = document.querySelector('.demographics-section');
            
            if (demographicsSection) {
                // Always show demographics section
                demographicsSection.style.display = 'block';
                
                // Auto-load demographics when account changes
                if (value !== 'Todos') {
                    setTimeout(() => loadDemographics(), 100);
                } else {
                    // Clear demographics for Todos
                    const segmentsDiv = document.getElementById('segments-distribution');
                    const tableBody = document.getElementById('demographics-table');
                    if (segmentsDiv) {
                        segmentsDiv.innerHTML = '<div style="padding: 20px; text-align: center; color: #86868b; font-size: 14px;">📊 Selecciona una cuenta específica para ver datos demográficos</div>';
                    }
                    if (tableBody) tableBody.innerHTML = '';
                }
                
            }
            
            updateDashboard(currentPeriod);
        }
        
        let keyboardBuffer = '';
        let keyboardTimer = null;
        
        function toggleDropdown() {
            const menu = document.getElementById('account-dropdown-menu');
            const btn = document.getElementById('account-dropdown-btn');
            const arrow = btn.querySelector('svg');
            
            if (menu.style.display === 'none' || !menu.style.display) {
                menu.style.display = 'block';
                arrow.style.transform = 'translateY(-50%) rotate(180deg)';
                btn.style.background = 'white';
                btn.style.color = '#2563eb';
                btn.style.borderColor = 'white';
                
                // Focus sur la recherche
                const searchInput = document.getElementById('account-search');
                if (searchInput) setTimeout(()=> searchInput.focus(), 0);
                
                // Activer la navigation clavier
                document.addEventListener('keydown', handleKeyboardNavigation);
            } else {
                closeDropdown();
            }
        }
        
        function handleKeyboardNavigation(e) {
            const menu = document.getElementById('account-dropdown-menu');
            if (menu.style.display === 'none' || !menu.style.display) return;
            
            // Navigation par lettres
            if (e.key.length === 1 && e.key.match(/[a-zA-Z0-9]/)) {
                // Accumuler les lettres tapées
                keyboardBuffer += e.key.toLowerCase();
                
                // Reset le buffer après 1 seconde
                clearTimeout(keyboardTimer);
                keyboardTimer = setTimeout(() => {
                    keyboardBuffer = '';
                }, 1000);
                
                // Trouver la première option qui commence par ces lettres
                const options = menu.querySelectorAll('.dropdown-option');
                for (let i = 0; i < options.length; i++) {
                    const option = options[i];
                    const text = option.textContent.toLowerCase();
                    if (text.startsWith(keyboardBuffer)) {
                        // Scroll le menu (pas la page!) pour mettre cette option en haut
                        const optionTop = option.offsetTop;
                        menu.scrollTop = optionTop;
                        
                        // La surligner temporairement
                        options.forEach(o => o.style.background = '');
                        option.style.background = '#f5f5f7';
                        break;
                    }
                }
            }
            
            // Escape pour fermer
            if (e.key === 'Escape') {
                closeDropdown();
            }
            
            // Enter pour sélectionner l'option survolée
            if (e.key === 'Enter') {
                const hoveredOption = menu.querySelector('.dropdown-option:hover');
                if (hoveredOption) {
                    const value = hoveredOption.getAttribute('data-value');
                    selectAccount(value);
                    closeDropdown();
                }
            }
        }
        
        function closeDropdown() {
            const menu = document.getElementById('account-dropdown-menu');
            const btn = document.getElementById('account-dropdown-btn');
            const arrow = btn.querySelector('svg');
            
            menu.style.display = 'none';
            arrow.style.transform = 'translateY(-50%)';
            btn.style.background = 'rgba(255,255,255,0.2)';
            btn.style.color = 'white';
            btn.style.borderColor = 'rgba(255,255,255,0.3)';
            
            // Désactiver la navigation clavier
            document.removeEventListener('keydown', handleKeyboardNavigation);
            keyboardBuffer = '';
        }
        
        // Mode client: fonctions utilitaires
        function getQueryParams(){
            const p = new URLSearchParams(window.location.search);
            return {
                account: p.get('account') ? decodeURIComponent(p.get('account')) : null,
                locked: p.get('locked') === '1'
            };
        }

        function updateShareButton() {
            // Supprimer le bouton existant s'il y en a un
            const existingBtn = document.getElementById('share-btn');
            if (existingBtn) {
                existingBtn.remove();
            }
            
            // Ajouter le bouton seulement si un compte spécifique est sélectionné
            if (currentAccountName && currentAccountName !== 'Todos') {
                addShareButton();
            }
        }
        
        function addShareButton(){
            const container = document.querySelector('.account-selector');
            if (!container) return;
            const btn = document.createElement('button');
            btn.id = 'share-btn';
            btn.className = 'dropdown-btn';
            btn.style.cssText = `
                margin-left: 8px;
                background: rgba(255,255,255,0.15);
                border: 2px solid rgba(255,255,255,0.25);
                color: white;
                padding: 10px 18px;
                border-radius: 25px;
                cursor: pointer;
                transition: all 0.3s ease;
                font-weight: 600;
                font-size: 14px;
                display: inline-flex;
                align-items: center;
                gap: 8px;
            `;
            btn.innerHTML = `
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"></path>
                    <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"></path>
                </svg>
                <span>Compartir</span>
            `;
            
            // Hover effect
            btn.onmouseover = () => {
                btn.style.background = 'rgba(255,255,255,0.25)';
                btn.style.borderColor = 'rgba(255,255,255,0.4)';
                btn.style.transform = 'translateY(-2px)';
            };
            btn.onmouseout = () => {
                btn.style.background = 'rgba(255,255,255,0.15)';
                btn.style.borderColor = 'rgba(255,255,255,0.25)';
                btn.style.transform = 'translateY(0)';
            };
            
            btn.onclick = () => {
                if (!currentAccountName || currentAccountName === 'Todos') {
                    alert('Selecciona una cuenta primero');
                    return;
                }
                const url = `${location.origin}${location.pathname}?account=${encodeURIComponent(currentAccountName)}&locked=1`;
                if (navigator.clipboard) {
                    navigator.clipboard.writeText(url).then(()=>{
                        btn.innerHTML = `
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polyline points="20 6 9 17 4 12"></polyline>
                            </svg>
                            <span>Copiado!</span>
                        `;
                        btn.style.background = 'rgba(76, 217, 100, 0.3)';
                        btn.style.borderColor = 'rgba(76, 217, 100, 0.5)';
                        setTimeout(()=> {
                            btn.innerHTML = `
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"></path>
                                    <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"></path>
                                </svg>
                                <span>Compartir</span>
                            `;
                            btn.style.background = 'rgba(255,255,255,0.15)';
                            btn.style.borderColor = 'rgba(255,255,255,0.25)';
                        }, 2000);
                    }).catch(()=>{
                        prompt('Copia el enlace para compartir:', url);
                    });
                } else {
                    prompt('Copia el enlace para compartir:', url);
                }
            };
            container.appendChild(btn);
        }

        function showShareBadge(accName){
            const hdr = document.querySelector('.header');
            if (!hdr) return;
            const badge = document.createElement('div');
            badge.className = 'share-badge';
            badge.textContent = `🔒 Modo cliente: ${accName}`;
            hdr.appendChild(badge);
        }

        // Tutorial Modal Functions
        const TUTORIAL_VIDEOS = {
            intro: {
                id: '4ecbc5cb11144dec931d5285a6e70cb7',
                title: '📚 Tutorial del Dashboard'
            },
            nomenclature: {
                id: 'c368e56148ce49b3b3735f7617d6c883',
                title: '📝 Cómo Nomenclaturar tus Anuncios'
            }
        };

        function openTutorialModal(videoKey) {
            const video = TUTORIAL_VIDEOS[videoKey];
            if (!video) return;

            const modal = document.getElementById('tutorial-modal');
            const titleEl = document.getElementById('tutorial-modal-title');
            const iframeEl = document.getElementById('tutorial-modal-iframe');

            if (modal && titleEl && iframeEl) {
                titleEl.textContent = video.title;
                iframeEl.src = `https://www.loom.com/embed/${video.id}?autoplay=1`;
                modal.classList.add('active');
                document.body.style.overflow = 'hidden';
            }
        }

        function closeTutorialModal() {
            const modal = document.getElementById('tutorial-modal');
            const iframeEl = document.getElementById('tutorial-modal-iframe');

            if (modal) {
                modal.classList.remove('active');
                document.body.style.overflow = '';
            }
            if (iframeEl) {
                iframeEl.src = ''; // Stop video
            }
        }

        // ======================================================================
        // ONBOARDING SYSTEM
        // ======================================================================
        const ONBOARDING_KEYS = {
            welcomeSeen: 'saas_welcome_seen_v1',
            tutorialClicked: 'saas_tutorial_clicked'
        };

        function initOnboarding() {
            // Check if this is a first-time user (never saw welcome modal)
            const welcomeSeen = localStorage.getItem(ONBOARDING_KEYS.welcomeSeen);

            if (!welcomeSeen) {
                // First visit: show welcome modal after a short delay
                setTimeout(() => {
                    showWelcomeModal();
                }, 1500);
            }

            // Check if tutorial button should have pulsing badge
            updateTutorialBadge();
        }

        function showWelcomeModal() {
            const modal = document.getElementById('welcome-modal');
            if (modal) {
                modal.classList.add('active');
                document.body.style.overflow = 'hidden';
            }
        }

        function closeWelcomeModal() {
            const modal = document.getElementById('welcome-modal');
            if (modal) {
                modal.classList.remove('active');
                document.body.style.overflow = '';
            }
            // Mark welcome as seen
            localStorage.setItem(ONBOARDING_KEYS.welcomeSeen, Date.now().toString());
        }

        function watchWelcomeTutorial() {
            closeWelcomeModal();
            // Mark tutorial as clicked (removes pulsing badge)
            localStorage.setItem(ONBOARDING_KEYS.tutorialClicked, Date.now().toString());
            updateTutorialBadge();
            // Open the intro tutorial
            openTutorialModal('intro');
        }

        function updateTutorialBadge() {
            const btn = document.getElementById('tutorial-header-btn');
            if (!btn) return;

            const tutorialClicked = localStorage.getItem(ONBOARDING_KEYS.tutorialClicked);

            if (!tutorialClicked) {
                // User hasn't clicked tutorial yet - show pulsing badge
                btn.classList.add('unread');
            } else {
                btn.classList.remove('unread');
            }
        }

        // Override openTutorialModal to track when user clicks tutorial
        const _originalOpenTutorialModal = typeof openTutorialModal === 'function' ? openTutorialModal : null;

        function openTutorialModalWithTracking(type) {
            // Mark tutorial as clicked
            localStorage.setItem(ONBOARDING_KEYS.tutorialClicked, Date.now().toString());
            updateTutorialBadge();

            // Call original function
            if (_originalOpenTutorialModal) {
                _originalOpenTutorialModal(type);
            } else {
                // Fallback: original implementation
                const modal = document.getElementById('tutorial-modal');
                const titleEl = document.getElementById('tutorial-modal-title');
                const iframeEl = document.getElementById('tutorial-modal-iframe');

                if (!modal || !iframeEl) return;

                const tutorials = {
                    intro: {
                        title: 'Tutorial: Cómo usar el Dashboard',
                        url: 'https://www.loom.com/embed/4ecbc5cb11144dec931d5285a6e70cb7?autoplay=1'
                    },
                    nomenclature: {
                        title: 'Tutorial: Cómo nomenclaturar tus ads',
                        url: 'https://www.loom.com/embed/c368e56148ce49b3b3735f7617d6c883?autoplay=1'
                    }
                };

                const tut = tutorials[type] || tutorials.intro;
                if (titleEl) titleEl.textContent = tut.title;
                iframeEl.src = tut.url;
                modal.classList.add('active');
                document.body.style.overflow = 'hidden';
            }
        }
        // Show contextual alert when nomenclature coverage is too low
        function showNomenclatureAlert(section, coverage) {
            // Replace the section content with a helpful alert
            section.innerHTML = `
                <div class="nomenclature-alert">
                    <div class="nomenclature-alert-icon">📋</div>
                    <h4>¡Desbloquea insights más potentes!</h4>
                    <p>
                        ${coverage === 0
                            ? 'Ninguno de tus anuncios sigue la nomenclatura.'
                            : `Solo el <strong>${coverage.toFixed(0)}%</strong> de tus anuncios siguen la nomenclatura.`}
                        Con una buena nomenclatura podrás ver análisis por ángulo, creador y hook.
                    </p>
                    <div class="nomenclature-alert-format">
                        <code>Tipo / Ángulo / Creador / Edad / Hook</code>
                        <span class="nomenclature-alert-example">Ej: Nuevo / Picazon / Maria / 35+ / H1</span>
                    </div>
                    <button class="nomenclature-alert-btn" onclick="openTutorialModal('nomenclature'); localStorage.setItem(ONBOARDING_KEYS.tutorialClicked, Date.now().toString()); updateTutorialBadge();">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <circle cx="12" cy="12" r="10"></circle>
                            <polygon points="10 8 16 12 10 16 10 8" fill="currentColor"></polygon>
                        </svg>
                        Ver Tutorial (9 min)
                    </button>
                </div>
            `;
        }
        // ======================================================================
        // END ONBOARDING SYSTEM
        // ======================================================================

        function addTutorialButton() {
            const container = document.querySelector('.account-selector');
            if (!container || document.getElementById('tutorial-header-btn')) return;

            const btn = document.createElement('button');
            btn.id = 'tutorial-header-btn';
            btn.className = 'tutorial-header-btn';
            btn.innerHTML = `
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <circle cx="12" cy="12" r="10"></circle>
                    <polygon points="10 8 16 12 10 16 10 8"></polygon>
                </svg>
                <span>Tutorial</span>
            `;
            btn.onclick = () => {
                // Track tutorial click (removes pulsing badge)
                localStorage.setItem(ONBOARDING_KEYS.tutorialClicked, Date.now().toString());
                updateTutorialBadge();
                openTutorialModal('intro');
            };
            container.appendChild(btn);

            // Apply pulsing badge if user hasn't watched tutorial yet
            updateTutorialBadge();
        }

        function addNomenclatureLink() {
            // Target the main table using the title ID
            const tableTitle = document.getElementById('table-title');
            if (!tableTitle || document.getElementById('nomenclature-help-link')) return;

            const titleContainer = tableTitle.parentElement;
            const subtitle = titleContainer.querySelector('p');
            if (!subtitle) return;

            // Create the link with separator included
            const link = document.createElement('span');
            link.id = 'nomenclature-help-link';
            link.className = 'nomenclature-help-link';
            link.innerHTML = `
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <circle cx="12" cy="12" r="10"></circle>
                    <polygon points="10 8 16 12 10 16 10 8"></polygon>
                </svg>
                ¿Cómo nomenclaturar?
            `;
            link.onclick = () => openTutorialModal('nomenclature');

            // Make subtitle inline and append link after it
            subtitle.style.display = 'inline';
            subtitle.after(link);
        }

        // Event listeners pour le dropdown custom
        document.addEventListener('DOMContentLoaded', () => {
            const btn = document.getElementById('account-dropdown-btn');
            if (btn) {
                btn.addEventListener('click', toggleDropdown);
            }
            
            // Fermer le dropdown si on clique ailleurs
            document.addEventListener('click', (e) => {
                const dropdown = document.querySelector('.custom-dropdown');
                if (dropdown && !dropdown.contains(e.target)) {
                    closeDropdown();
                }
            });
        });
        
        // Écouteur pour les selects (filtres et résultats)
        document.addEventListener('change', (e) => {
            // Filtres de tableau
            if (e.target && e.target.classList.contains('table-filter')) {
                console.log('Filtre changé:', e.target, 'Valeur:', e.target.value);
                window.applyTableFilters();
            }
            
            // Sauvegarder les résultats (Ganador/Perdedor)
            if (e.target && e.target.classList.contains('result-select')) {
                const adId = e.target.getAttribute('data-ad-id');
                const value = e.target.value;
                
                const savedData = JSON.parse(localStorage.getItem('adHypotheses') || '{}');
                if (!savedData[adId]) savedData[adId] = {};
                savedData[adId].result = value;
                localStorage.setItem('adHypotheses', JSON.stringify(savedData));
                
                // Auto-sugerir plantilla en Aprendizajes si está vacío
                const row = e.target.closest('tr');
                const learningEl = row ? row.querySelector('.learning-input') : null;
                if (learningEl && !learningEl.value.trim()) {
                    const tpl =
                        value === 'ganador'
                            ? 'Ganador. Aprendizaje: ¿qué funcionó (hook/ángulo/creador)? Siguiente paso: escalar +20-30% y probar 2 variantes (hook/CTA).'
                            : value === 'perdedor'
                                ? 'Perdedor. Aprendizaje: ¿qué no funcionó? Acciones: pausar o limitar entrega; iterar 2 variantes (hook/ángulo/creador) y re-test en 7 días.'
                                : '';
                    if (tpl) {
                        learningEl.value = tpl;
                        // auto-resize
                        learningEl.style.height = '36px';
                        learningEl.style.height = Math.min(learningEl.scrollHeight, 100) + 'px';
                        // guardar
                        const sd = JSON.parse(localStorage.getItem('adHypotheses') || '{}');
                        if (!sd[adId]) sd[adId] = {};
                        sd[adId].learning = learningEl.value;
                        localStorage.setItem('adHypotheses', JSON.stringify(sd));
                    }
                }
                
                // Si un filtre de Resultado est actif, rafraîchir la vue
                if (typeof window.applyTableFilters === 'function') window.applyTableFilters();
            }
            
            // Save awareness level
            if (e.target && e.target.classList.contains('awareness-select')) {
                const adId = e.target.getAttribute('data-ad-id');
                const value = e.target.value;
                
                const savedData = JSON.parse(localStorage.getItem('adHypotheses') || '{}');
                if (!savedData[adId]) savedData[adId] = {};
                savedData[adId].awarenessLevel = value;
                localStorage.setItem('adHypotheses', JSON.stringify(savedData));
                
                // Refresh filters if needed
                if (typeof window.applyTableFilters === 'function') window.applyTableFilters();
            }
            
            // Sauvegarder Tipo de Hook (manuel)
            if (e.target && e.target.classList.contains('hook-select')) {
                const adId = e.target.getAttribute('data-ad-id');
                const value = e.target.value;
                
                const savedData = JSON.parse(localStorage.getItem('adHypotheses') || '{}');
                if (!savedData[adId]) savedData[adId] = {};
                savedData[adId].hookType = value;
                localStorage.setItem('adHypotheses', JSON.stringify(savedData));
                
                // Rafraîchir le cache de filtres et les graphiques hooks
                refreshHypothesesCache();
                if (window.currentData) updateHookSection(window.currentData);
                
                // Si un filtre "Tipo de Hook" est actif, on applique
                if (typeof window.applyTableFilters === 'function') window.applyTableFilters();
            }
        });
        
        // Écouteur pour les inputs text (filtres et hypothèses)
        document.addEventListener('input', (e) => {
            // Filtres de tableau (text inputs)
            if (e.target && e.target.classList.contains('table-filter')) {
                window.applyTableFilters();
            }
            
            // Sauvegarder les hypothèses
            if (e.target && e.target.classList.contains('hypothesis-input')) {
                const adId = e.target.getAttribute('data-ad-id');
                const value = e.target.value;
                
                const savedData = JSON.parse(localStorage.getItem('adHypotheses') || '{}');
                if (!savedData[adId]) savedData[adId] = {};
                savedData[adId].hypothesis = value;
                localStorage.setItem('adHypotheses', JSON.stringify(savedData));
            }
            
            // Sauvegarder los aprendizajes
            if (e.target && e.target.classList.contains('learning-input')) {
                const adId = e.target.getAttribute('data-ad-id');
                const value = e.target.value;
                const savedData = JSON.parse(localStorage.getItem('adHypotheses') || '{}');
                if (!savedData[adId]) savedData[adId] = {};
                savedData[adId].learning = value;
                localStorage.setItem('adHypotheses', JSON.stringify(savedData));
            }
        });
        
        // Export to Excel functionality
        function exportToExcel() {
            const savedData = JSON.parse(localStorage.getItem('adHypotheses') || '{}');
            
            // Prepare CSV content
            let csv = '\ufeff'; // UTF-8 BOM for Excel
            csv += 'Anuncio,Cuenta,Estado,Formato,Fecha Inicio,Importe Gastado,ROAS,CPA,Compras,CTR,CPM,Tipo de Hook,Hipótesis,Aprendizajes,Resultado\n';
            
            filteredTableData.forEach(ad => {
                const hypothesis = savedData[ad.ad_id]?.hypothesis || '';
                const learning = savedData[ad.ad_id]?.learning || '';
                const hookType = savedData[ad.ad_id]?.hookType || '';
                const result = savedData[ad.ad_id]?.result || '';
                const startDate = ad.created_time ? new Date(ad.created_time).toLocaleDateString('es-MX') : '';
                const ctr = (parseFloat(ad.clicks || 0) / parseFloat(ad.impressions || 1) * 100).toFixed(2);
                
                // Utilise escapeCsv définie ligne 1298 (plus sécurisée)
                
                csv += [
                    escapeCsv(ad.ad_name),
                    escapeCsv(ad.account_name),
                    ad.effective_status,
                    ad.format,
                    startDate,
                    Math.round(parseFloat(ad.spend || 0)),
                    ad.roas.toFixed(2),
                    ad.cpa > 0 ? ad.cpa.toFixed(0) : '',
                    ad.purchases || 0,
                    ctr,
                    ad.cpm > 0 ? ad.cpm.toFixed(0) : '',
                    escapeCsv(hookType),
                    escapeCsv(hypothesis),
                    escapeCsv(learning),
                    result === 'ganador' ? 'Ganador' : result === 'perdedor' ? 'Perdedor' : ''
                ].join(',') + '\n';
            });
            
            // Create blob and download
            const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
            const link = document.createElement('a');
            const url = URL.createObjectURL(blob);
            
            link.setAttribute('href', url);
            link.setAttribute('download', `insights_${currentPeriod}d_${new Date().toISOString().split('T')[0]}.csv`);
            link.style.visibility = 'hidden';
            
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        }
        
        // Add event listener for export button and demographics
        document.addEventListener('click', (e) => {
            if (e.target && e.target.id === 'export-excel') {
                exportToExcel();
            }
            
            // Demographics button removed - now auto-loads
        });

        // Demographics loading function - Load pre-calculated JSON from API
        async function loadDemographics() {
            // Show loading state
            const segmentsDiv = document.getElementById('segments-distribution');
            const tableBody = document.getElementById('demographics-table');
            
            if (segmentsDiv) {
                segmentsDiv.innerHTML = '<div style="padding: 20px; text-align: center;"><span style="color: #86868b;">⏳ ' + t('dashboard.loading_messages.loading_demographics') + '</span></div>';
            }
            
            try {
                // Check if Todos is selected
                if (!currentAccountName || currentAccountName === 'Todos') {
                    // Hide demographics for Todos
                    if (segmentsDiv) {
                        segmentsDiv.innerHTML = '<div style="padding: 20px; text-align: center; color: #86868b; font-size: 14px;">📊 Selecciona una cuenta específica para ver datos demográficos</div>';
                    }
                    if (tableBody) tableBody.innerHTML = '';
                    return;
                }
                
                // Get account ID from current data
                let accountDir = null;
                if (window.currentData && window.currentData.ads && window.currentData.ads.length > 0) {
                    const sampleAd = window.currentData.ads.find(ad => 
                        ad.account_name === currentAccountName
                    );
                    if (sampleAd && sampleAd.account_id) {
                        const idRaw = String(sampleAd.account_id);
                        // Normalize: always use act_ prefix
                        accountDir = idRaw.startsWith('act_') ? idRaw : `act_${idRaw}`;
                    }
                }
                
                if (!accountDir) {
                    // No account ID found
                    if (segmentsDiv) {
                        segmentsDiv.innerHTML = '<div style="padding: 20px; text-align: center; color: #86868b; font-size: 14px;">⚠️ No se pudo identificar el ID de la cuenta</div>';
                    }
                    if (tableBody) tableBody.innerHTML = '';
                    return;
                }
                
                // Use exact period - we now fetch all 5 periods: 3, 7, 14, 30, 90
                const period = currentPeriod || 7;

                console.log(`Loading demographics for ${accountDir} (${period}d)...`);

                // 🔄 SaaS mode: Load from API (if loadDemographicsFromAPI exists from data_loader_saas.js)
                let data = null;
                if (typeof loadDemographicsFromAPI === 'function') {
                    data = await loadDemographicsFromAPI(accountDir, period);
                } else {
                    // Fallback: Load from static files (legacy Patrons mode)
                    const jsonPath = `./data/demographics/${accountDir}/${period}d.json`;
                    console.log(`Fetching static: ${jsonPath}`);
                    const response = await fetch(jsonPath, { cache: 'no-store' });
                    if (response.ok) {
                        data = await response.json();
                    }
                }

                if (!data) {
                    throw new Error('Demographics data not available');
                }
                
                // Afficher les données réelles
                displayDemographicsData(data);
                
            } catch (error) {
                console.error('Error loading demographics:', error);
                
                // Show error message instead of demo data
                
                const tableBody = document.getElementById('demographics-table');
                const segmentsDiv = document.getElementById('segments-distribution');
                
                if (segmentsDiv) {
                    segmentsDiv.innerHTML = `
                        <div style="padding: 20px; background: #fff3cd; border-radius: 8px; text-align: center;">
                            <p style="color: #856404; margin-bottom: 12px; font-size: 14px;">
                                ⚠️ Datos demográficos no disponibles para esta cuenta y período
                            </p>
                            <p style="color: #856404; font-size: 13px; margin: 0;">
                                Los datos se generan automáticamente varias veces al día.<br>
                                Por favor, intenta más tarde.
                            </p>
                        </div>
                    `;
                }
                
                if (tableBody) {
                    tableBody.innerHTML = '';
                }
            }
        }
        
        // Function to display real demographics data from API
        function displayDemographicsData(data) {
            if (!data || !data.segments) return;
            
            // Demographics are always visible now (no button to hide)
            
            // Display table with all segments (EXACTLY like Facebook's view)
            // Using more lenient ROAS thresholds:
            // ROAS >= 2.0 = green ✅
            // ROAS >= 1.2 = orange ⚠️  
            // ROAS < 1.2 = red ❌
            const tableBody = document.getElementById('demographics-table');
            tableBody.innerHTML = data.segments.map(seg => {
                const roasColor = seg.roas >= 2.0 ? '#00a854' : seg.roas >= 1.2 ? '#ff9500' : '#ff3b30';
                const roasIcon = seg.roas >= 2.0 ? '✅' : seg.roas >= 1.2 ? '⚠️' : '❌';
                const segmentName = seg.age ? `${seg.age} ${seg.gender}` : seg.gender;
                return `
                    <tr>
                        <td style="font-weight: 500;">${segmentName}</td>
                        <td style="font-weight: 600;">$${seg.spend.toLocaleString('es-MX', {minimumFractionDigits: 2})}</td>
                        <td style="color: ${roasColor}; font-weight: 600;">${seg.roas.toFixed(2)} ${roasIcon}</td>
                        <td>${seg.ctr.toFixed(2)}%</td>
                        <td>$${seg.cpa > 0 ? seg.cpa.toFixed(2) : 'N/A'}</td>
                        <td>${seg.purchases}</td>
                    </tr>
                `;
            }).join('');
            
            // Clear the segments chart div (no longer showing redundant chart)
            const segmentsDiv = document.getElementById('segments-distribution');
            
            // Add summary stats only
            const totalSpend = data.segments.reduce((sum, s) => sum + s.spend, 0);
            const totalPurchases = data.segments.reduce((sum, s) => sum + s.purchases, 0);
            const avgRoas = totalSpend > 0 ? (data.segments.reduce((sum, s) => sum + s.purchase_value, 0) / totalSpend) : 0;
            
            segmentsDiv.innerHTML = `
                <div style="padding: 20px; background: #f8f9fa; border-radius: 8px;">
                    <div style="display: flex; justify-content: space-around; text-align: center;">
                        <div>
                            <div style="font-size: 20px; font-weight: 700; color: #1d1d1f;">$${(totalSpend/1000).toFixed(1)}k</div>
                            <div style="font-size: 12px; color: #86868b;">Inversión Total</div>
                        </div>
                        <div>
                            <div style="font-size: 20px; font-weight: 700; color: ${avgRoas >= 2.0 ? '#00a854' : avgRoas >= 1.2 ? '#ff9500' : '#ff3b30'};">${avgRoas.toFixed(2)}</div>
                            <div style="font-size: 12px; color: #86868b;">ROAS Promedio</div>
                        </div>
                        <div>
                            <div style="font-size: 20px; font-weight: 700; color: #667eea;">${totalPurchases}</div>
                            <div style="font-size: 12px; color: #86868b;">Conversiones</div>
                        </div>
                    </div>
                </div>
            `;
        }
        
        // Fonction de filtrage
        // Fonction de filtrage supprimée - utilise window.applyTableFilters définie ligne 1862

        // Charger les données au démarrage
        // Configuration des profils de compte
        let accountProfiles = {};
        async function loadAccountProfiles() {
            try {
                const response = await fetch('config/account_profiles.json');
                if (response.ok) {
                    const config = await response.json();
                    accountProfiles = config;
                }
            } catch (e) {
                console.log('No account profiles config found, using default');
                accountProfiles = { profiles: {}, default: 'ecom' };
            }
        }

        // Déterminer le profil du compte actuel
        function getAccountProfile(accountId) {
            if (!accountId) return accountProfiles.default || 'ecom';

            // Chercher l'ID exact ou le nom du compte
            if (accountProfiles.profiles) {
                // D'abord chercher par ID exact
                if (accountProfiles.profiles[accountId]) {
                    return accountProfiles.profiles[accountId];
                }
                // Si c'est le compte Ads-Alchemy par ID
                if (accountId === 'act_3816311378380297' || accountId === '3816311378380297') {
                    return 'leads';
                }
            }
            return accountProfiles.default || 'ecom';
        }

        // i18n: Update dynamic elements when translations are loaded
        document.addEventListener('i18n:ready', () => {
            // Update document title
            document.title = t('dashboard.page_title');

            // Update placeholders for inputs (data-i18n-placeholder)
            document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
                const key = el.getAttribute('data-i18n-placeholder');
                el.placeholder = t(key);
            });
        });

        // Session validation - redirect to landing if no auth token
        function validateSession() {
            const token = localStorage.getItem('auth_token');
            if (!token) {
                console.warn('🔒 No auth token found, redirecting to login...');
                window.location.href = 'index-landing.html';
                return false;
            }
            return true;
        }

        document.addEventListener('DOMContentLoaded', async () => {
            // Validate session before anything else
            if (!validateSession()) return;

            // Charger la configuration des profils
            await loadAccountProfiles();

            // Afficher "Chargement..." dans les KPIs
            const kpiCards = document.querySelectorAll('.kpi-content h3');
            kpiCards.forEach(card => card.textContent = '...');

            // Mode client (URL)
            const qp = getQueryParams();
            if (qp.account) {
                currentAccountName = qp.account; // verrouille le filtre dès le départ
                const btnText = document.getElementById('account-dropdown-text');
                if (btnText) btnText.textContent = currentAccountName.length > 28 ? currentAccountName.substring(0,25) + '...' : currentAccountName;
            }

            await loadAllData();

            // Sorting header listeners
            initTableSorting();

            buildAccountOptions();

            // Tutorial buttons (always visible)
            addTutorialButton();
            addNomenclatureLink();

            // Si locked=1, masquer le dropdown et afficher le badge
            if (qp.locked) {
                const dropdown = document.querySelector('.custom-dropdown');
                if (dropdown) dropdown.style.display = 'none';
                showShareBadge(currentAccountName || 'Cuenta');
            } else {
                // Bouton de partage visible seulement si un compte est sélectionné
                if (currentAccountName && currentAccountName !== 'Todos') {
                    addShareButton();
                }
            }
        });
