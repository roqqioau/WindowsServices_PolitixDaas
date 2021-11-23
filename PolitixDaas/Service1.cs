﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace PolitixDaas
{
    public partial class Service1 : ServiceBase
    {
        Timer timer = null;
        DUtils dUtils = null;
        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            Logging.lstErrors.Clear();
            Logging.WriteLog("Roqqio Daas Service Beginning OnStart.");
            int i = 0;
            try
            {
                dUtils = new DUtils();
                timer = new Timer();
                this.timer.Interval = dUtils.dcSetup.IntervalMins * 60 * 1000;

                timer.Elapsed += new System.Timers.ElapsedEventHandler(timer_tick);
                timer.Enabled = true;
                Logging.WriteLog("Roqqio Daas Service started.");

            }
            catch (Exception e)
            {
                Logging.WriteErrorLog("Start Error " + i.ToString() + " " + e.Message);
            }

        }

        private void timer_tick(Object sender, ElapsedEventArgs e)
        {
            if (sender != null)
                timer.Enabled = false;
            Logging.lstErrors.Clear();
            Logging.WriteLog("Timer task starts!");
            try
            {
                dUtils.process();

            }
            catch (Exception e1)
            {
                Logging.WriteErrorLog(e1.Message);
            }

            try
            {
                dUtils.sendEmail(Logging.getErrors());
                if (sender != null)
                    timer.Enabled = true;
            }
            catch (Exception em)
            {
                Logging.WriteErrorLog(em.Message);
                if (sender != null)
                    timer.Enabled = true;
            }
        }


        protected override void OnStop()
        {
            Logging.WriteLog("Roqqio Daas Windows Service OnStop.");
        }

        public void Starty()
        {
            OnStart(null);
            timer_tick(null, null);
        }

        public void Stoppy()
        {
            OnStop();
        }


    }

}
