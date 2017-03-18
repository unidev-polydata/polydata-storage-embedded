package com.unidev.polydata;

import com.unidev.platform.j2ee.common.WebUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.web.filter.HiddenHttpMethodFilter;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;

@SpringBootApplication
@EnableDiscoveryClient
public class Application extends WebSecurityConfigurerAdapter implements ServletContextInitializer  {

	public static final String VERSION = "0.0.1";

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public WebUtils webUtils() {
		return new WebUtils();
	}

    @Override
    public void onStartup(ServletContext servletContext) throws ServletException {
    }

	@Bean
	public ServletRegistrationBean jminixServlet() {
		ServletRegistrationBean servletBean = new ServletRegistrationBean();
		servletBean.addUrlMappings("/jmx/*");
		servletBean.setServlet(new org.jminix.console.servlet.MiniConsoleServlet());
		return servletBean;
	}

	@Bean
	public FilterRegistrationBean excludeDefaultFilter(HiddenHttpMethodFilter filter) {
		FilterRegistrationBean registration = new FilterRegistrationBean(filter);
		registration.setEnabled(false);
		return registration;
	}


	@Value("${admin.user}")
	private String adminUser;

	@Value("${admin.password}")
	private String adminPassword;

	@Override
	protected void configure(HttpSecurity http) throws Exception {
		http
				.authorizeRequests()
				.antMatchers("/api/**").permitAll()
				.anyRequest().authenticated().and().formLogin().and().csrf().disable().cors().disable();
	}

	@Autowired
	public void configureGlobal(AuthenticationManagerBuilder auth) throws Exception {
		auth
				.inMemoryAuthentication()
				.withUser(adminUser).password(adminPassword).roles("ADMIN");
	}

}

